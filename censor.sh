if [ -z "${CI_MERGE_REQUEST_TARGET_BRANCH_SHA}" ]
then
  echo "Not something that can be merged with this MR!"
  exit -1
fi
echo "base-commit: ${CI_MERGE_REQUEST_TARGET_BRANCH_SHA}"
echo "target-commit ${CI_COMMIT_SHA}"

/opt/docker/bin/censor ${CENSOR_ARGS} \
--repo-path '.' \
--base-commit ${CI_MERGE_REQUEST_TARGET_BRANCH_SHA} \
--target-commit ${CI_COMMIT_SHA} \
--exclude 'documentation/icons/WASP_logo.pdf' \
--exclude 'documentation/diagrams/statemachines.png' \
--exclude 'icons/WASP_logo.pdf' \
--exclude 'diagrams/statemachines.png' \
--exclude 'censor.sh'
