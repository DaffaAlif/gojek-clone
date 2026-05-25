pipeline {
    agent any

    options {
        timeout(time: 15, unit: 'MINUTES')
        buildDiscarder(logRotator(numToKeepStr: '10'))
    }

    environment {
        VENV         = '.venv'
        REPORTS_DIR  = 'reports'
        SIM_TIMEOUT  = '120'
    }

    stages {

        // ── 1. Checkout ─────────────────────────────────────────────────────
        stage('Checkout') {
            steps {
                echo '── Checkout kode ──'
                checkout scm
            }
        }

        // ── 2. Setup Python virtualenv ───────────────────────────────────────
        stage('Setup Environment') {
            steps {
                echo '── Membuat virtualenv & install dependencies ──'
                sh '''
                    python3 -m venv ${VENV}
                    ${VENV}/bin/pip install --upgrade pip
                    ${VENV}/bin/pip install -r requirements.txt
                '''
            }
        }

        // ── 3. Start Kafka via docker-compose ────────────────────────────────
        stage('Start Kafka') {
            steps {
                echo '── Menjalankan Kafka & Zookeeper ──'
                sh 'docker-compose up -d'
                // Tunggu Kafka siap (health-check sederhana)
                sh '''
                    echo "Menunggu Kafka siap..."
                    for i in $(seq 1 30); do
                        if docker exec kafka kafka-topics \
                            --bootstrap-server localhost:9092 \
                            --list > /dev/null 2>&1; then
                            echo "Kafka siap setelah ${i}s"
                            break
                        fi
                        sleep 2
                    done
                '''
            }
        }

        // ── 4. Unit Tests (tidak butuh Kafka) ────────────────────────────────
        stage('Unit Tests') {
            steps {
                echo '── Menjalankan unit tests ──'
                sh '''
                    mkdir -p ${REPORTS_DIR}
                    ${VENV}/bin/pytest tests/test_unit.py \
                        -v \
                        --tb=short \
                        --junitxml=${REPORTS_DIR}/unit-tests.xml \
                        --cov=. \
                        --cov-report=xml:${REPORTS_DIR}/coverage.xml \
                        --cov-report=term-missing
                '''
            }
            post {
                always {
                    junit "${REPORTS_DIR}/unit-tests.xml"
                }
            }
        }

        // ── 5. Integration Tests (butuh Kafka) ───────────────────────────────
        stage('Integration Tests') {
            steps {
                echo '── Menjalankan integration tests ──'
                sh '''
                    ${VENV}/bin/pytest tests/test_integration.py \
                        -v \
                        --tb=short \
                        --timeout=60 \
                        --junitxml=${REPORTS_DIR}/integration-tests.xml
                '''
            }
            post {
                always {
                    junit "${REPORTS_DIR}/integration-tests.xml"
                }
            }
        }

        // ── 6. Simulasi 1 Driver + 1 Rider ───────────────────────────────────
        stage('Simulation: 1 Driver + 1 Rider') {
            steps {
                echo '── Menjalankan simulasi penuh 1 driver + 1 rider ──'
                sh '''
                    PYTHONUTF8=1 ${VENV}/bin/python simulate_one_ride.py \
                        --timeout ${SIM_TIMEOUT}
                '''
            }
        }

        // ── 7. E2E Real-World Test (60 detik) ────────────────────────────────
        stage('E2E Real-World Test (60s)') {
            steps {
                echo '── Menjalankan end-to-end test 60 detik dengan 3 driver + 3 rider ──'
                sh '''
                    PYTHONUTF8=1 ${VENV}/bin/pytest tests/test_e2e_realworld.py \
                        -v \
                        -s \
                        --timeout=90 \
                        --junitxml=${REPORTS_DIR}/e2e-tests.xml
                '''
            }
            post {
                always {
                    junit "${REPORTS_DIR}/e2e-tests.xml"
                }
            }
        }

    }

    // ── Post-pipeline actions ────────────────────────────────────────────────
    post {

        always {
            echo '── Mematikan infrastruktur Kafka ──'
            sh 'docker-compose down --volumes --remove-orphans || true'

            // Arsip laporan
            archiveArtifacts artifacts: 'reports/*.xml', allowEmptyArchive: true
        }

        success {
            echo "✅ Pipeline BERHASIL — build #${BUILD_NUMBER}"
        }

        failure {
            echo "❌ Pipeline GAGAL — build #${BUILD_NUMBER}"
        }

        unstable {
            echo "⚠️  Pipeline UNSTABLE (ada test yang gagal) — build #${BUILD_NUMBER}"
        }
    }
}
