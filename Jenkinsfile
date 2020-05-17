pipeline {//mainbranch......
    agent any
    tools {
        maven 'Maven3.6.3'
    }

    stages {
        stage ('checkout') {
            steps {
                checkout scm
            }
        }

        stage ('Pack jar') {
            steps {
                bat "mvnw package -P main && mvn dependency:tree -P main"
            }
        }

        stage ('Run Kafka') {
            steps {
                bat """cd /d D:\\kafkaTest\\docker-zookeeper
                docker-compose build
                docker-compose up -d"""
            }
        }

        stage ('Create docker image') {
            steps {
                bat "docker build -t springio/gs-spring-boot-docker ."
            }
        }

        stage ('Services UP') {
            steps {
                bat "docker run -p 9091:9091 --name MyService -d -v D://NtustMaster//First//Project//CIMFORCE//testFile//temp://files 8353c27acabe"
            }     
        }
        
        stage ('Services down Trigger') {
            steps {
                input message: 'Shut down the services?'
            }
        }
    }

    post {
        always {
            bat 'docker stop MyService && docker rm MyService'
        }
    }
}