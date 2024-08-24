FROM ruby:3.3.0-bullseye

RUN apt-get update && apt-get install -y libxml2-dev redis-tools

WORKDIR /app

ENV OPENSSL_CONF=/etc/ssl/

COPY Gemfile reqless.gemspec .
COPY lib/reqless/version.rb lib/reqless/version.rb

RUN NOKOGIRI_USE_SYSTEM_LIBRARIES=1 bundle install

CMD bundle exec rake
