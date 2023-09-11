buildMvn {
  publishModDescriptor = true
  mvnDeploy = false
  buildNode =  'jenkins-agent-java17'

  doDocker = {
    buildJavaDocker {
      publishMaster = true
      healthChk = true
      healthChkCmd = 'wget --no-verbose --tries=1 --spider http://localhost:8081/admin/health || exit 1'
    }
  }

}
