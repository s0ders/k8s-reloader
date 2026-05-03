package controller

var (
	// When set, trigger reload onto changes of whatever ConfigMap or Secret the deployment uses, except for those
	// who set the ignore annotation on themselves.
	reloaderEnabledAnnotation = "reloader.k8s.akira.sh/enabled"

	// Comma separated list of ConfigMap to trigger reload onto changes.
	reloaderConfigMapEnabledAnnotation = "configmap.reloader.k8s.akira.sh/reload"
	// Comma separated list of Secret to trigger reload onto changes.
	reloaderSecretEnabledAnnotation = "secret.reloader.k8s.akira.sh/reload"

	// Used to trigger a rollout on workloads.
	reloaderTimestampAnnotation = "reloader.k8s.akira.sh/reloadedAt"

	// Used on secrets and configmaps to ignore reload on changes globally.
	reloaderIgnoreAnnotation = "reloader.k8s.akira.sh/ignore"
)

func configMapHashAnnotation(name string) string {
	return "configmap.reloader.k8s.akira.sh/" + name + "-hash"
}

func secretHashAnnotation(name string) string {
	return "secret.reloader.k8s.akira.sh/" + name + "-hash"
}
