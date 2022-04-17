FROM golang:latest AS builder

WORKDIR /go/src
RUN git clone https://github.com/magefile/mage && cd mage && go run bootstrap.go
COPY ./ .
RUN mage -v build

FROM apify/actor-node-puppeteer-chrome:16

COPY --from=builder --chown=myuser:myuser /go/src/import-sa-quant-rank .

# Second, copy just package.json and package-lock.json since it should be
# the only file that affects "npm install" in the next step, to speed up the build
COPY --chown=myuser:myuser scraper/package*.json ./

# Install NPM packages, skip optional and development dependencies to
# keep the image small. Avoid logging too much and print the dependency
# tree for debugging
RUN npm --quiet set progress=false \
 && npm install --only=prod --no-optional \
 && echo "Installed NPM packages:" \
 && (npm list --only=prod --no-optional --all || true) \
 && echo "Node.js version:" \
 && node --version \
 && echo "NPM version:" \
 && npm --version

# Next, copy the remaining files and directories with the source code.
# Since we do this after NPM install, quick build will be really fast
# for most source file changes.
COPY --chown=myuser:myuser scraper/* ./

# Optionally, specify how to launch the source code of your actor.
# By default, Apify's base Docker images define the CMD instruction
# that runs the Node.js source code using the command specified
# in the "scripts.start" section of the package.json file.
# In short, the instruction looks something like this:
#
# CMD npm start
