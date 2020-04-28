if [ -z "${CI_MERGE_REQUEST_TARGET_BRANCH_NAME}" ]
then
  echo "Not something that can be merged with this MR!"
  exit -1
fi
echo "base-commit: ${CI_MERGE_REQUEST_TARGET_BRANCH_NAME}"
echo "target-commit ${CI_COMMIT_SHA}"

censor ${CENSOR_ARGS} \
--repo-path '.' \
--base-commit origin/${CI_MERGE_REQUEST_TARGET_BRANCH_NAME} \
--target-commit ${CI_COMMIT_SHA} \
--exclude 'documentation/icons/WASP_logo.pdf' \
--exclude 'documentation/diagrams/statemachines.png' \
--exclude 'icons/WASP_logo.pdf' \
--exclude 'diagrams/statemachines.png' \
--exclude 'censor.sh'
