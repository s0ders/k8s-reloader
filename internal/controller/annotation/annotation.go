package annotation

var (
	// ReloaderEnabledAnnotation when set to true, trigger reload onto changes of whatever ConfigMap or Secret the
	// workload uses, except for those who set the ignore annotation on themselves.
	ReloaderEnabledAnnotation = "reloader.k8s.akira.sh/enabled"

	// ReloaderConfigMapEnabledAnnotation is a comma separated list of ConfigMap to trigger reload onto changes.
	ReloaderConfigMapEnabledAnnotation = "configmap.reloader.k8s.akira.sh/reload"

	// ReloaderSecretEnabledAnnotation is a comma separated list of Secret to trigger reload onto changes.
	ReloaderSecretEnabledAnnotation = "secret.reloader.k8s.akira.sh/reload"

	// ReloaderTimestampAnnotation is used to trigger a rollout on workloads.
	ReloaderTimestampAnnotation = "reloader.k8s.akira.sh/reloadedAt"

	// ReloaderIgnoreAnnotation is used on secrets and configmaps to ignore reload on changes globally.
	ReloaderIgnoreAnnotation = "reloader.k8s.akira.sh/ignore"
)

func ConfigMapHashAnnotation(name string) string {
	return "configmap.reloader.k8s.akira.sh/" + name + "-hash"
}

func SecretHashAnnotation(name string) string {
	return "secret.reloader.k8s.akira.sh/" + name + "-hash"
}
