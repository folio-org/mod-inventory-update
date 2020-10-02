buildMvn {
  publishModDescriptor = true
  mvnDeploy = false
  publishAPI = true
  runLintRamlCop = true
  buildNode =  'jenkins-agent-java11'

  doDocker = {
    buildJavaDocker {
      publishMaster = true
      healthChk = false
      healthChkCmd = 'curl -sS --fail -o /dev/null  http://localhost:8081/apidocs/ || exit 1'
    }
  }

}
