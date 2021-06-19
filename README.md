# 개요
Advanced Analytics with Spark 도서에 대한 스터디를 최신 버전의 스파크를 이용하여 다시 구현해보는 방식으로 진행해보고자 합니다. 특히 **PySpark**를 이용하여 기존에 Scala로 작성된 소스를 따라가면서 똑같은 결과를 얻는 것에 초점을 맞췄습니다. 그리고 저수준 API인 RDD 사용은 줄이면서 최대한 DataFrame을 이용하여 결과를 얻을 수 있도록 정리하면서 공부하는 것이 목적입니다.

# Windows 개발 환경 구성
본래 윈도우에서 개발을 진행하려면 아래와 같이 Spark 개발 환경이 필요합니다. 그 밖에도 Python이나 Hadoop, Jupyter와 같은 패키지를 추가로 설치해야 할 필요가 있습니다. 윈도우에서는 이런 패키지들을 하나하나 따로 받아서 설청해야 하는 과정들이 여간 귀찮은게 아닙니다. 물론 Chocoletey를 사용한다면 좀 나을까요?

제가 아는 한, 이 모든 것을 한 번에 손쉽게 설치하는 방법은 Docker를 통해서 미리 구축된 이미지를 사용하는 것입니다.

* https://github.com/jupyter/docker-stacks
* https://hub.docker.com/r/jupyter/all-spark-notebook

이처럼 Jupyter에서 제공되는 Docker 이미지 중에서 편의상 all-spark-notebook을 이용하면 다음과 같이 `docker compose up` 명령을 통해서 사전 정의된 컨테이너 템플릿을 이용하여 JupyterLab을 실행할 수 있습니다.

본 이미지에서 사용하는 라이브러리 버전은 다음과 같습니다. 

* Python - 3.9.4
* Spark - 3.1.2
* JupyterLab - 3.0.16

이후 Docker 이미지가 정상적으로 구동된다면, 아래와 같은 메시지를 확인할 수 있습니다.

```
jupyter_1  | Executing the command: jupyter lab
jupyter_1  | [I 2021-06-19 04:25:32.145 ServerApp] jupyterlab | extension was successfully linked.
jupyter_1  | [W 2021-06-19 04:25:32.157 NotebookApp] 'ip' has moved from NotebookApp to ServerApp. This config will be passed to ServerApp. Be sure to update your config before our next release.
jupyter_1  | [W 2021-06-19 04:25:32.157 NotebookApp] 'port' has moved from NotebookApp to ServerApp. This config will be passed to ServerApp. Be sure to update your config before our next release.
jupyter_1  | [W 2021-06-19 04:25:32.157 NotebookApp] 'port' has moved from NotebookApp to ServerApp. This config will be passed to ServerApp. Be sure to update your config before our next release.
jupyter_1  | [I 2021-06-19 04:25:32.707 ServerApp] ipyparallel.nbextension | extension was found and enabled by nbclassic. Consider moving the extension to Jupyter Server's extension paths.
jupyter_1  | [I 2021-06-19 04:25:32.708 ServerApp] ipyparallel.nbextension | extension was successfully linked.
jupyter_1  | [I 2021-06-19 04:25:32.708 ServerApp] nbclassic | extension was successfully linked.
jupyter_1  | [I 2021-06-19 04:25:32.769 ServerApp] nbclassic | extension was successfully loaded.
jupyter_1  | [I 2021-06-19 04:25:32.769 ServerApp] Loading IPython parallel extension
jupyter_1  | [I 2021-06-19 04:25:32.771 ServerApp] ipyparallel.nbextension | extension was successfully loaded.
jupyter_1  | [I 2021-06-19 04:25:32.773 LabApp] JupyterLab extension loaded from /opt/conda/lib/python3.9/site-packages/jupyterlab
jupyter_1  | [I 2021-06-19 04:25:32.773 LabApp] JupyterLab application directory is /opt/conda/share/jupyter/lab
jupyter_1  | [I 2021-06-19 04:25:32.787 ServerApp] jupyterlab | extension was successfully loaded.
jupyter_1  | [I 2021-06-19 04:25:32.789 ServerApp] Serving notebooks from local directory: /home/jovyan
jupyter_1  | [I 2021-06-19 04:25:32.789 ServerApp] Jupyter Server 1.8.0 is running at:
jupyter_1  | [I 2021-06-19 04:25:32.789 ServerApp] http://fc06d84b79dd:8888/lab?token=0907472eebecb0c418af151d1d0de6d0fc9b67456994487c
jupyter_1  | [I 2021-06-19 04:25:32.790 ServerApp]     http://127.0.0.1:8888/lab?token=0907472eebecb0c418af151d1d0de6d0fc9b67456994487c
jupyter_1  | [I 2021-06-19 04:25:32.790 ServerApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
jupyter_1  | [C 2021-06-19 04:25:32.824 ServerApp]
jupyter_1  |
jupyter_1  |     To access the server, open this file in a browser:
jupyter_1  |         file:///home/jovyan/.local/share/jupyter/runtime/jpserver-7-open.html
jupyter_1  |     Or copy and paste one of these URLs:
jupyter_1  |         http://fc06d84b79dd:8888/lab?token=0907472eebecb0c418af151d1d0de6d0fc9b67456994487c
jupyter_1  |         http://127.0.0.1:8888/lab?token=0907472eebecb0c418af151d1d0de6d0fc9b67456994487c
```

그러면, 마지막 주소를 인터넷 브라우저에 복사해서 JupyterLab에 접근할 수 있는데 그렇게 하지 마시고 아래 주소로 접근합니다.

```
http://localhost:8889/lab
```

이제 마지막의 주소의 토큰을 복사해서 하단의 패스워드 설정을 진행하신 다음에 그 패스워드를 이용해서 JupyterLab 에 접근하는 것이 더 좋습니다. 이렇게 해서 윈도우 재부팅 후에도 같은 Docker 이미지를 사용하여 로그인을 할 수 있습니다. 재부팅시에 Docker는 정지되어 있을 수 있는데, 이 때는 Docker 명령을 이용해서 실행하면 됩니다.

```
docker start jupyterlab
```
