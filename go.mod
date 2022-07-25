module github.com/jpbetz/conversion-webhook-example

go 1.12

require (
	github.com/imdario/mergo v0.3.7 // indirect
	github.com/jamiealquiza/tachymeter v2.0.0+incompatible
	k8s.io/api v0.24.3
	k8s.io/apiextensions-apiserver v0.24.3
	k8s.io/apimachinery v0.24.3
	k8s.io/client-go v0.24.3
	sigs.k8s.io/yaml v1.2.0
)

replace (
	k8s.io/api => k8s.io/api v0.24.3
	k8s.io/client-go => k8s.io/client-go v0.24.3
)
