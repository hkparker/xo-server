FROM node:6-wheezy@sha256:5eb189fa5041cb692146d3212593ef965e8adf58f48a2bfc738fa30bb7f5045f

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install build-essential libpng-dev git python-minimal -y

RUN git clone -b stable http://github.com/vatesfr/xo-server /opt/xo-server && \
    git clone -b stable http://github.com/vatesfr/xo-web /opt/xo-web

RUN cd /opt/xo-server && npm install && npm run build && \
    cd /opt/xo-web && npm install && npm run build

RUN cd /opt/xo-server && cp sample.config.yaml .xo-server.yaml && \
    sed -i -e "s/#'\/': '\/path\/to\/xo-web\/dist\/'/'\/': '\/opt\/xo-web\/dist\/'/g" .xo-server.yaml && \
    sed -i -e "s/#uri: ''/uri: 'redis:\/\/redis:6379'/g" .xo-server.yaml && \
    sed -i -e "#user: 'nobody'/user: 'nobody'" .xo-server.yaml && \
    sed -i -e "#group: 'nogroup'/group: 'nogroup'" .xo-server.yaml

CMD cd /opt/xo-server && npm start
