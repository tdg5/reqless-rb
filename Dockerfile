FROM ruby:3.3.0-bullseye

RUN apt-get update && apt-get install -y libxml2-dev redis-tools

WORKDIR /app

ENV OPENSSL_CONF=/etc/ssl/

RUN wget https://bitbucket.org/ariya/phantomjs/downloads/phantomjs-2.1.1-linux-x86_64.tar.bz2 \
  && tar xjf phantomjs-2.1.1-linux-x86_64.tar.bz2 \
  && mv phantomjs-2.1.1-linux-x86_64/bin/phantomjs /usr/local/bin/phantomjs

COPY Gemfile qless.gemspec .
COPY lib/qless/version.rb lib/qless/version.rb

RUN NOKOGIRI_USE_SYSTEM_LIBRARIES=1 bundle install

CMD bundle exec rake
