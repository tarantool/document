FROM tarantool/tarantool:1.7

COPY . /opt/tarantool/

WORKDIR /opt/tarantool

RUN set -x \
    && apk add --no-cache --virtual .build-deps \
       git bash cmake make gcc musl-dev \
    && ./deps.sh \
    && tarantoolctl rocks make rockspecs/document-scm-1.rockspec \
    && : "---------- remove build deps ----------" \
    && apk del .build-deps


COPY *.lua /opt/tarantool/
