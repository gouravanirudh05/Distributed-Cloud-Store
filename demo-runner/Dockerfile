# Runs demo.sh scenarios for the web UI's "Demo scenarios" buttons (needs the Docker socket).
FROM docker:27.3.1-cli
RUN apk add --no-cache bash curl jq coreutils diffutils gawk python3 ffmpeg zip unzip 7zip xz
WORKDIR /project
COPY demo.sh /project/demo.sh
COPY demo-runner/runner.py /usr/local/bin/runner.py
ENV PROJECT_DIR=/project API=http://gateway:8080/api/v1 PORT=8090
EXPOSE 8090
ENTRYPOINT ["python3", "-u", "/usr/local/bin/runner.py"]
