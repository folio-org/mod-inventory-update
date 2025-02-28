buildMvn {
  publishModDescriptor = true
  mvnDeploy = false
  buildNode =  'jenkins-agent-java21'

  doDocker = {
    buildJavaDocker {
      publishMaster = true
      healthChk = true
      healthChkCmd = 'wget --no-verbose --tries=1 --spider http://localhost:8080/admin/health || exit 1'
    }
  }

}
