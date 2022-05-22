FROM golang AS builder
WORKDIR /go/src
RUN git clone https://github.com/magefile/mage && cd mage && go run bootstrap.go
COPY ./ .
RUN mage -v build

FROM pennyvault/playwright-go
COPY --from=builder --chown=playwright:playwright /go/src/import-sa-quant-rank /home/playwright
COPY --chown=playwright:playwright start_xvfb_and_run_cmd.sh /home/playwright/

WORKDIR /home/playwright

ENV DISPLAY=:99
ENV XVFB_WHD=1280x720x16

CMD ./start_xvfb_and_run_cmd.sh && /home/playwright/import-sa-quant-rank
