buildMvn {
  publishModDescriptor = true
  mvnDeploy = false
  publishAPI = true
  buildNode =  'jenkins-agent-java11'

  doApiLint = true
  apiTypes = 'RAML'
  apiDirectories = 'ramls'

  doDocker = {
    buildJavaDocker {
      publishMaster = true
      healthChk = false
      healthChkCmd = 'curl -sS --fail -o /dev/null  http://localhost:8081/apidocs/ || exit 1'
    }
  }

}
