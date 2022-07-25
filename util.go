package main

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apiextensionsclientsetv1 "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/typed/apiextensions/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	clientv1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"sigs.k8s.io/yaml"
)

var (
	// GVR used for building dynamic client
	foov1GVR     = schema.GroupVersionResource{Group: "stable.example.com", Version: "v1", Resource: "foos"}
	endpointsGVR = schema.GroupVersionResource{Version: "v1", Resource: "endpoints"}
	notfoundGVR  = schema.GroupVersionResource{Version: "error", Resource: "notfound"}

	emptyNamespace         = "empty"
	largeDataNamespace     = "large-data"
	largeMetadataNamespace = "large-metadata"

	fooName = "foos.stable.example.com"

	// size in kB
	largeDataSize = 10000
	dummyFields   = []string{"spec", "data"}

	// number of objects we will create and list in list benchmarks
	testListSize = 1000
)

var foov1Template = []byte(`apiVersion: stable.example.com/v1
kind: Foo
metadata:
  name: template
spec:
  data: "abc123,d4,"`)

var endpointsTemplate = []byte(`apiVersion: v1
kind: Endpoints
metadata:
  name: template`)

var validationSchema = []byte(`openAPIV3Schema:
  type: object
  properties:
   spec:
     type: object
     x-kubernetes-preserve-unknown-fields: true
     x-kubernetes-validations:
       - rule: "self.data.matches(r'^([a-z]+[0-9]+,)+$')"
         message: "data must be in expected format."
     properties:
       data:
         type: string`)

// mustNewRESTConfig builds a rest client config
func mustNewRESTConfig() *rest.Config {
	// TODO: add flag support in TestMain for running in master VM / remotely
	kubeconfig := filepath.Join(homedir.HomeDir(), ".kube", "config")
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	// config, err := clientcmd.DefaultClientConfig.ClientConfig()
	if err != nil {
		panic(err)
	}
	// wait for long running requests, e.g. deleting 10k objects
	config.Timeout = 10 * time.Minute

	// increase QPS (default 5) for heavy load testing
	config.QPS = 10000
	config.Burst = 20000
	return config
}

// mustNewDynamicClient creates a new dynamic client
func mustNewDynamicClient() dynamic.Interface {
	client, err := dynamic.NewForConfig(mustNewRESTConfig())
	if err != nil {
		panic(err)
	}
	return client
}

// mustNewClientset creates a new clientset containing typed clients for groups
func mustNewClientset() *kubernetes.Clientset {
	client, err := kubernetes.NewForConfig(mustNewRESTConfig())
	if err != nil {
		panic(err)
	}
	return client
}

// BenchmarkClient provides create and list interface for benchmark testing
type BenchmarkClient interface {
	// use i to customize and avoid race
	Create(i int) (interface{}, error)
	List() (interface{}, error)
	Count() (int, error)
	Watch() (watch.Interface, error)
	DeleteCollection() error
}

var _ BenchmarkClient = &dynamicBenchmarkClient{}
var _ BenchmarkClient = &endpointsBenchmarkClient{}

// dynamicBenchmarkClient implements BenchmarkClient interface
type dynamicBenchmarkClient struct {
	client      dynamic.ResourceInterface
	template    *unstructured.Unstructured
	listOptions *metav1.ListOptions
}

func (c *dynamicBenchmarkClient) Create(i int) (interface{}, error) {
	obj := c.template.DeepCopy()
	obj.SetName(fmt.Sprintf("%d-%d", time.Now().Nanosecond(), i))
	return c.client.Create(context.TODO(), obj, metav1.CreateOptions{}, "")
}

func (c *dynamicBenchmarkClient) List() (interface{}, error) {
	return c.client.List(context.TODO(), *c.listOptions)
}

func (c *dynamicBenchmarkClient) Count() (int, error) {
	l, err := c.client.List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return 0, err
	}
	return len(l.Items), nil
}

func (c *dynamicBenchmarkClient) Watch() (watch.Interface, error) {
	return c.client.Watch(context.TODO(), *c.listOptions)
}

func (c *dynamicBenchmarkClient) DeleteCollection() error {
	return c.client.DeleteCollection(context.TODO(), metav1.DeleteOptions{}, metav1.ListOptions{})
}

// endpointsBenchmarkClient implements BenchmarkClient interface
type endpointsBenchmarkClient struct {
	client      clientv1.EndpointsInterface
	template    *v1.Endpoints
	listOptions *metav1.ListOptions
}

func (c *endpointsBenchmarkClient) Create(i int) (interface{}, error) {
	obj := c.template.DeepCopy()
	obj.SetName(fmt.Sprintf("%d-%d", time.Now().Nanosecond(), i))
	return c.client.Create(context.TODO(), obj, metav1.CreateOptions{})
}

func (c *endpointsBenchmarkClient) List() (interface{}, error) {
	return c.client.List(context.TODO(), *c.listOptions)
}

func (c *endpointsBenchmarkClient) Count() (int, error) {
	// list from etcd
	l, err := c.client.List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return 0, err
	}
	return len(l.Items), nil
}

func (c *endpointsBenchmarkClient) Watch() (watch.Interface, error) {
	return c.client.Watch(context.TODO(), *c.listOptions)
}

func (c *endpointsBenchmarkClient) DeleteCollection() error {
	return c.client.DeleteCollection(context.TODO(), metav1.DeleteOptions{}, metav1.ListOptions{})
}

func mustNewDynamicBenchmarkClient(gvr schema.GroupVersionResource, namespace string,
	templateData []byte, listOptions *metav1.ListOptions) BenchmarkClient {
	template := unstructured.Unstructured{}
	if err := yaml.Unmarshal(templateData, &template); err != nil {
		panic(err)
	}
	return &dynamicBenchmarkClient{
		client:      mustNewDynamicClient().Resource(gvr).Namespace(namespace),
		template:    &template,
		listOptions: listOptions,
	}
}

func mustNewEndpointsBenchmarkClient(namespace string, templateData []byte,
	listOptions *metav1.ListOptions) BenchmarkClient {
	template := v1.Endpoints{}
	if err := yaml.Unmarshal(templateData, &template); err != nil {
		panic(err)
	}
	return &endpointsBenchmarkClient{
		client:      mustNewClientset().CoreV1().Endpoints(namespace),
		template:    &template,
		listOptions: listOptions,
	}
}

// FIXME: generate large data
// mustIncreaseObjectSize bumps data by kB size
func mustIncreaseObjectSize(data []byte, size int, fields ...string) []byte {
	u := unstructured.Unstructured{}
	if err := yaml.Unmarshal(data, &u); err != nil {
		panic(err)
	}
	// NOTE: we are have a rough equivalence in size between annotation and CR array,
	// because there is no good array candidate in metadata
	// TODO: unstructured doesn't support set (deep copy) nested struct
	letters := "abcdefghijklmnopqrstuvwxyz"
	digits := "0123456789"
	var sb strings.Builder
	for true {
		max_length := size - sb.Len() - 1
		if max_length < 2 {
			break
		}
		part_length := rand.Intn(19) + 2
		if part_length > max_length {
			part_length = max_length
		}
		num_letters := part_length / 2
		num_digits := part_length - num_letters
		for i := 0; i < num_letters; i += 1 {
			sb.WriteRune(rune(letters[rand.Intn(len(letters))]))
		}
		for i := 0; i < num_digits; i += 1 {
			sb.WriteRune(rune(digits[rand.Intn(len(digits))]))
		}
		sb.WriteRune(',')
	}
	generated_data := sb.String()
	if err := unstructured.SetNestedField(u.Object, generated_data, fields...); err != nil {
		panic(err)
	}
	d, err := yaml.Marshal(&u)
	if err != nil {
		panic(err)
	}
	return d
}

func getGVR(name string) schema.GroupVersionResource {
	if strings.Contains(name, "CR") {
		return foov1GVR
	}
	if strings.Contains(name, "Endpoints") && strings.Contains(name, "Dynamic") {
		return endpointsGVR
	}
	return notfoundGVR
}

func getNamespace(name string) string {
	if strings.Contains(name, "LargeData") {
		return largeDataNamespace
	}
	if strings.Contains(name, "LargeMetadata") {
		return largeMetadataNamespace
	}
	return emptyNamespace
}

func getTemplate(name string) []byte {
	var template []byte
	if strings.Contains(name, "CR") {
		template = foov1Template
	} else {
		template = endpointsTemplate
	}

	if strings.Contains(name, "LargeData") {
		template = mustIncreaseObjectSize(template, largeDataSize, dummyFields...)
	}
	return template
}

func getListOptions(name string) *metav1.ListOptions {
	if strings.Contains(name, "WatchCache") {
		return &metav1.ListOptions{ResourceVersion: "0"}
	}
	return &metav1.ListOptions{}
}

func setupNamespace(name string) {
	c := mustNewClientset().CoreV1().Namespaces()
	_, err := c.Get(context.TODO(), name, metav1.GetOptions{})
	if err == nil {
		return
	}
	if errors.IsNotFound(err) {
		if _, err := c.Create(context.TODO(), &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}, metav1.CreateOptions{}); err != nil {
			panic(err)
		}
		// wait for namespace to be initialized
		time.Sleep(10 * time.Second)
	} else {
		panic(err)
	}
}

func setupValidation(enable bool) {
	clientset, err := apiextensionsclientset.NewForConfig(mustNewRESTConfig())
	if err != nil {
		panic(err)
	}
	client := clientset.ApiextensionsV1().CustomResourceDefinitions()
	if enable {
		v := apiextensionsv1.CustomResourceValidation{}
		if err := yaml.Unmarshal(validationSchema, &v); err != nil {
			panic(err)
		}
		mustHaveValidation(client, fooName, &v)
	} else {
		mustHaveValidation(client, fooName, nil)
	}
}

// mustHaveValidation makes sure given CRD has expected validation set / unset
func mustHaveValidation(client apiextensionsclientsetv1.CustomResourceDefinitionInterface, name string,
	validation *apiextensionsv1.CustomResourceValidation) {
	crd, err := client.Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		panic(err)
	}
	for i, version := range crd.Spec.Versions {
		if version.Name == "v1" && !apiequality.Semantic.DeepEqual(validation, version.Schema) {
			crd.Spec.Versions[i].Schema = validation
		}
	}
	if _, err := client.Update(context.TODO(), crd, metav1.UpdateOptions{}); err != nil {
		panic(err)
	}
	// wait for potential initialization
	time.Sleep(5 * time.Second)
}

func ensureObjectCount(client BenchmarkClient, listSize int) error {
	num, err := client.Count()
	if err != nil {
		return fmt.Errorf("failed to check list size: %v", err)
	}
	if num < listSize {
		var wg sync.WaitGroup
		remaining := listSize - num
		wg.Add(remaining)
		for i := 0; i < remaining; i++ {
			// deep copy i
			idx := i
			go func() {
				_, err := client.Create(idx)
				if err != nil {
					// TODO: b.Fatal doesn't raise
					// b.Fatalf("failed to create object: %v", err)
					panic(err)
				}
				wg.Done()
			}()
		}
		wg.Wait()
	} else if num > listSize {
		return fmt.Errorf("Too many items already exist. Want %d got %d", listSize, num)
	}
	return nil
}
