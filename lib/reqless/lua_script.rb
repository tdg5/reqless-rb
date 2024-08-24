# Encoding: utf-8

require 'digest/sha1'

module Reqless
  LuaScriptError = Class.new(Reqless::Error)

  # Wraps a lua script. Knows how to reload it if necessary
  class LuaScript
    DEFAULT_ON_RELOAD_CALLBACK = proc {}
    SCRIPT_ROOT = File.expand_path('../lua', __FILE__)

    def initialize(name, redis, options = {})
      @name  = name
      @on_reload_callback = options[:on_reload_callback] || DEFAULT_ON_RELOAD_CALLBACK
      @redis = redis
      @sha   = Digest::SHA1.hexdigest(script_contents)
    end

    attr_reader :name, :redis, :sha

    def reload
      @sha = @redis.script(:load, script_contents)
      @on_reload_callback.call(@redis, @sha)
      @sha
    end

    def call(*argv)
      handle_no_script_error do
        _call(*argv)
      end
    rescue Redis::CommandError => err
      if match = err.message.match('user_script:\d+:\s*(\w+.+$)')
        raise LuaScriptError.new(match[1])
      else
        raise err
      end
    end

  private
    def _call(*argv)
      @redis.evalsha(@sha, keys: [], argv: argv)
    end

    def handle_no_script_error
      yield
    rescue ScriptNotLoadedRedisCommandError
      reload
      yield
    end

    # Module for notifying when a script hasn't yet been loaded
    module ScriptNotLoadedRedisCommandError
      MESSAGE = 'NOSCRIPT No matching script. Please use EVAL.'

      def self.===(error)
        error.is_a?(Redis::CommandError) && error.message.include?(MESSAGE)
      end
    end

    def script_contents
      @script_contents ||= File.read(File.join(SCRIPT_ROOT, "#{@name}.lua"))
    end
  end

  # Provides a simple way to load and use lua-based Reqless plugins.
  # This combines the reqless-lib.lua script plus your custom script
  # contents all into one script, so that your script can use
  # Reqless's lua API.
  class LuaPlugin < LuaScript
    def initialize(name, redis, plugin_contents)
      @name  = name
      @redis = redis
      @plugin_contents = plugin_contents.gsub(COMMENT_LINES_RE, '')
      super(name, redis)
    end

  private

    def script_contents
      @script_contents ||= [REQLESS_LIB_CONTENTS, @plugin_contents].join("\n\n")
    end

    COMMENT_LINES_RE = /^\s*--.*$\n?/

    REQLESS_LIB_CONTENTS = File.read(
      File.join(SCRIPT_ROOT, 'reqless-lib.lua')
    ).gsub(COMMENT_LINES_RE, '')
  end
end
