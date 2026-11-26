#!/usr/bin/env bash
# À lancer juste avant chaque capture d'écran : affiche l'identité Onyxia,
# le namespace Kubernetes et la date/heure (exigé pour que la preuve compte).
NS_FILE=/var/run/secrets/kubernetes.io/serviceaccount/namespace
NS=$( [ -f "$NS_FILE" ] && cat "$NS_FILE" || echo "${KUBERNETES_NAMESPACE:-inconnu}" )
echo "=============================================="
echo " Onyxia user   : ${BUCKET:-?} (bucket)"
echo " Namespace k8s : ${NS}"
echo " Pod           : ${HOSTNAME:-?}"
echo " Date / heure  : $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo "=============================================="
