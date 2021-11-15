buildMvn {
  publishModDescriptor = true
  mvnDeploy = false
  buildNode =  'jenkins-agent-java11'

  doApiLint = true
  doApiDoc = true
  apiTypes = 'RAML'
  apiDirectories = 'ramls'

  doDocker = {
    buildJavaDocker {
      publishMaster = true
      healthChk = true
      healthChkCmd = 'curl -sS --fail -o /dev/null http://localhost:8080/admin/health || exit 1'
    }
  }

}
