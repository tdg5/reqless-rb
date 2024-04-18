# Encoding: utf-8

ENV['RACK_ENV'] = 'test'
require 'spec_helper'
require 'yaml'
require 'qless'
require 'qless/server'
require 'capybara/rspec'
require 'capybara/poltergeist'
require 'rack/test'

Capybara.javascript_driver = :poltergeist
Capybara.default_max_wait_time = 2

def try_repeatedly(max_attempts = 10)
    attempts = 0
    while attempts < max_attempts && !yield
      attempts += 1
      sleep 0.1
    end
    if attempts == max_attempts
      puts '[WARNING] try_repeatedly exhausted attempts:'
      puts(caller)
    end
end

module Qless
  describe Server, :integration, type: :request do
    # Our main test queue
    let(:q) { client.queues['testing'] }
    # Point to the main queue, but identify as different workers
    let(:a) { client.queues['testing'].tap { |o| o.worker_name = 'worker-a' } }
    let(:b) { client.queues['testing'].tap { |o| o.worker_name = 'worker-b' } }
    # And a second queue
    let(:other)  { client.queues['other']   }

    before(:all) do
      Capybara.app = Qless::Server.new(Qless::Client.new(redis_config))
    end

    # Ensure the phantomjs process doesn't live past these tests.
    # Otherwise, they are additional child processes that interfere
    # with the tests for the forking server, since it uses
    # `wait2` to wait on any child process.
    after(:all) do
      Capybara.using_driver(:poltergeist) do
        Capybara.current_session.driver.quit
      end
    end

    it 'can visit each top-nav tab' do
      visit '/'

      all_tabs = [
        'About',
        'Completed',
        'Config',
        'Failed',
        'Queues',
        'Throttles',
        'Track',
        'Workers',
      ]

      all_tabs.each do |tab_title|
        link = first('ul.nav a', text: tab_title)
        click_link link.text
      end
    end

    def build_paginated_objects
      # build 30 since our page size is 25 so we have at least 2 pages
      30.times do |i|
        yield "jid-#{i + 1}"
      end
    end

    # We put periods on the end of these jids so that
    # an assertion about "jid-1" will not pass if "jid-11"
    # is on the page. The jids are formatted as "#{jid}..."
    def assert_page(present_jid_num, absent_jid_num)
      expect(page).to have_content("jid-#{present_jid_num}.")
      expect(page).to_not have_content("jid-#{absent_jid_num}.")
    end

    def click_pagination_link(text)
      within '.top-pagination' do
        click_link text
      end
    end

    def test_pagination(page_1_jid = 1, page_2_jid = 27)
      assert_page page_1_jid, page_2_jid

      click_pagination_link 'Next'
      assert_page page_2_jid, page_1_jid

      click_pagination_link 'Prev'
      assert_page page_1_jid, page_2_jid
    end

    it 'can paginate a group of tagged jobs' do
      build_paginated_objects do |jid|
        q.put(Qless::Job, {}, tags: ['foo'], jid: jid)
      end

      visit '/tag?tag=foo'

      test_pagination
    end

    it 'can paginate the failed jobs page' do
      build_paginated_objects do |jid|
        q.put(Qless::Job, {}, jid: jid)
        q.pop.fail('group', 'msg')
      end

      visit '/failed/group'

      # The failed jobs page shows the jobs in reverse order, for some reason.
      test_pagination 20, 1
    end

    it 'can paginate the completed jobs page' do
      build_paginated_objects do |jid|
        q.put(Qless::Job, {}, :jid => jid)
        q.pop.complete
      end

      visit '/completed'

      # The completed jobs page shows the jobs in reverse order
      test_pagination 20, 1
    end

    it 'can paginate jobs on the depends tab' do
      q.put(Qless::Job, {}, jid: 'parent-job')

      build_paginated_objects do |jid|
        q.put(Qless::Job, {}, jid: jid, depends: ['parent-job'])
      end

      visit "/queues/#{CGI.escape(q.name)}/depends"

      test_pagination
    end

    it 'can paginate jobs on the throttled tab' do
      throttle_id = 'paginate-throttles-throttle'
      throttle = Throttle.new(throttle_id, client)
      throttle.maximum = 1

      q.put(Qless::Job, {}, throttles: [throttle_id])
      q.pop

      build_paginated_objects do |jid|
        q.put(Qless::Job, {}, jid: jid, throttles: [throttle_id])
        # These pops will fail and cause the jobs to become throttled
        q.pop
      end

      visit "/queues/#{CGI.escape(q.name)}/throttled"

      test_pagination
    end

    it 'can set and delete queue throttles', js: true do
      q.put(Qless::Job, {})

      expect(q.throttle.maximum).to eq(0)

      visit '/throttles'

      text_field_class = ".ql-q-#{q.name}-maximum"
      expect(page).to have_selector('td', text: /ql:q:#{q.name}/i)
      expect(first(text_field_class)['placeholder']).to eq('0')

      maximum = first(text_field_class)
      maximum.set(3)
      maximum.trigger('blur')

      try_repeatedly { q.throttle.maximum == 3 }

      expect(first(text_field_class)['value']).to eq('3')
      expect(q.throttle.maximum).to eq(3)

      first('button.btn-danger').click
      first('button.btn-danger').click

      try_repeatedly { q.throttle.maximum == 0 }

      expect(first(text_field_class)['placeholder']).to eq('0')
    end

    it 'can set the expiration for queue throttles', js: true do
      q.put(Qless::Job, {})
      expect(q.throttle.maximum).to eq(0)
      expect(q.throttle.ttl).to eq(-2)

      visit '/throttles'

      maximum_field_class = ".ql-q-#{q.name}-maximum"
      expiration_field_class = ".ql-q-#{q.name}-expiration"

      expect(page).to have_selector('td', text: /ql:q:#{q.name}/i)
      expect(first(expiration_field_class)['placeholder']).to eq('-2')

      maximum = first(maximum_field_class)
      maximum.set(3)
      maximum.trigger('blur')

      expect(first(maximum_field_class)['value']).to eq('3')
      try_repeatedly { q.throttle.maximum == 3 }
      expect(q.throttle.maximum).to eq(3)

      expiration = first(expiration_field_class)
      expiration.set(1)
      expiration.trigger('blur')

      try_repeatedly(15) { q.throttle.ttl == -2 }

      expect(q.throttle.ttl).to eq(-2)

      visit '/throttles'

      expect(first(maximum_field_class)['placeholder']).to eq('0')
      expect(first(expiration_field_class)['placeholder']).to eq('-2')
    end

    it 'can set and delete job throttles', js: true do
      throttle_id = 'delete-throttles-throttle'
      jid = q.put(Qless::Job, {}, throttles: [throttle_id])

      text_field_class = ".#{throttle_id}-maximum"
      throttle = Throttle.new(throttle_id, client)

      expect(throttle.maximum).to eq(0)

      visit "/jobs/#{jid}"

      expect(page).to have_content(throttle_id)
      expect(first(text_field_class)['placeholder']).to eq('0')

      maximum = first(text_field_class)
      maximum.set(3)
      maximum.trigger('blur')

      try_repeatedly { throttle.maximum == 3 }

      expect(first(text_field_class)['value']).to eq('3')
      expect(throttle.maximum).to eq(3)

      first('button.btn-danger.remove-throttle').click
      first('button.btn-danger.remove-throttle').click

      try_repeatedly { throttle.maximum == 0 }

      expect(first(text_field_class)['placeholder']).to eq('0')
    end

    it 'can set the expiration for job throttles', js: true do
      throttle_id = 'expire-throttles-throttle'
      jid = q.put(Qless::Job, {}, throttles: [throttle_id])

      maximum_field_class = ".#{throttle_id}-maximum"
      expiration_field_class = ".#{throttle_id}-expiration"
      throttle = Throttle.new(throttle_id, client)

      expect(throttle.maximum).to eq(0)
      expect(throttle.ttl).to eq(-2)

      visit "/jobs/#{jid}"

      expect(page).to have_content(throttle_id)
      expect(first(expiration_field_class)['placeholder']).to eq('-2')

      maximum = first(maximum_field_class)
      maximum.set(3)
      maximum.trigger('blur')

      try_repeatedly { throttle.maximum == 3 }

      expect(first(maximum_field_class)['value']).to eq('3')
      expect(throttle.maximum).to eq(3)

      expiration = first(expiration_field_class)
      expiration.set(1)
      expiration.trigger('blur')

      try_repeatedly(15) { throttle.ttl == -2 }

      visit "/jobs/#{jid}"

      expect(first(maximum_field_class)['placeholder']).to eq('0')
      expect(first(expiration_field_class)['placeholder']).to eq('-2')
    end

    it 'can see the root-level summary' do
      visit '/'

      # When qless is empty, we should see the default bit
      expect(page).to have_selector('h1', text: /no queues/i)
      expect(page).to have_selector('h1', text: /no failed jobs/i)
      expect(page).to have_selector('h1', text: /no workers/i)

      # Alright, let's add a job to a queue, and make sure we can see it
      q.put(Qless::Job, {})
      visit '/'
      expect(page).to have_selector('.queue-row', text: /testing/)
      expect(page).to have_selector('.queue-row', text: /0\D+1\D+0\D+0\D+0\D+0\D+0/)
      expect(page).to_not have_selector('h1', text: /no queues/i)
      expect(page).to have_selector('h1', text: /queues and their job counts/i)

      # Let's pop the job, and make sure that we can see /that/
      job = q.pop
      expect(job).to be

      visit '/'
      expect(page).to have_selector('.queue-row', text: /1\D+0\D+0\D+0\D+0\D+0\D+0/)
      expect(page).to have_selector('.worker-row', text: q.worker_name)
      expect(page).to have_selector('.worker-row', text: /1\D+0/i)

      # Let's complete the job, and make sure it disappears
      job.complete

      visit '/'
      expect(page).to have_selector('.queue-row', text: /0\D+0\D+0\D+0\D+0\D+0\D+0/)
      expect(page).to have_selector('.worker-row', text: /0\D+0/i)

      # Let's throttle a job, and make sure we see it
      client.throttles['one'].maximum = 1
      q.put(Qless::Job, {}, :throttles => ['one'])
      q.put(Qless::Job, {}, :throttles => ['one'])
      job1 = q.pop
      job2 = q.pop

      visit '/'
      expect(page).to have_selector('.queue-row', text: /1\D+0\D+1\D+0\D+0\D+0\D+0/)
      job1.complete
      q.pop.complete

      # Let's put and pop and fail a job, and make sure we see it
      q.put(Qless::Job, {})
      job = q.pop
      job.fail('foo-failure', 'bar')

      visit '/'
      expect(page).to have_selector('.failed-row', text: /foo-failure/)
      expect(page).to have_selector('.failed-row', text: /1/i)

      # And let's have one scheduled, and make sure it shows up accordingly
      jid = q.put(Qless::Job, {}, delay: 60)

      visit '/'
      expect(page).to have_selector('.queue-row', text: /0\D+0\D+0\D+1\D+0\D+0\D+0/)
      # And one that depends on that job
      q.put(Qless::Job, {}, depends: [jid])

      visit '/'
      expect(page).to have_selector('.queue-row', text: /0\D+0\D+0\D+1\D+0\D+1\D+0/)
    end

    it 'can visit the tracked page' do
      # We should be able to see all of the appropriate tabs,
      # We should be able to see all of the jobs
      jid = q.put(Qless::Job, {})
      client.jobs[jid].track

      visit '/track'
      # Make sure it appears under 'all', and 'waiting'
      expect(page).to have_selector('a', text: /all\W+1/i)
      expect(page).to have_selector('a', text: /waiting\W+1/i)
      # Now let's pop off the job so that it's running
      job = q.pop

      visit '/track'
      expect(page).to have_selector('a', text: /all\W+1/i)
      expect(page).to have_selector('a', text: /waiting\W+0/i)
      expect(page).to have_selector('a', text: /running\W+1/i)
      # Now let's complete the job and make sure it shows up again
      job.complete

      visit '/track'
      expect(page).to have_selector('a', text: /all\W+1/i)
      expect(page).to have_selector('a', text: /running\W+0/i)
      expect(page).to have_selector('a', text: /completed\W+1/i)
      job.untrack

      # And now for a throttled job
      client.throttles['one'].maximum = 1
      q.put(Qless::Job, {}, throttles: ['one'])
      job = client.jobs[q.put(Qless::Job, {}, throttles: ['one'])]
      job.track
      q.pop(2)

      visit '/track'
      expect(page).to have_selector('a', text: /all\W+1/i)
      expect(page).to have_selector('a', text: /throttled\W+1/i)
      job.untrack

      # And now for a scheduled job
      job = client.jobs[q.put(Qless::Job, {}, delay: 600)]
      job.track

      visit '/track'
      expect(page).to have_selector('a', text: /all\W+1/i)
      expect(page).to have_selector('a', text: /scheduled\W+1/i)
      job.untrack

      # And a failed job
      q.put(Qless::Job, {})
      job = q.pop
      job.track
      job.fail('foo', 'bar')

      visit '/track'
      expect(page).to have_selector('a', text: /all\W+1/i)
      expect(page).to have_selector('a', text: /failed\W+1/i)
      job.untrack

      # And a depends job
      job = client.jobs[
        q.put(Qless::Job, {}, depends: [q.put(Qless::Job, {})])]
      job.track

      visit '/track'
      expect(page).to have_selector('a', text: /all\W+1/i)
      expect(page).to have_selector('a', text: /depends\W+1/i)
      job.untrack
    end

    it 'can display the correct buttons for jobs' do
      # Depending on the state of the job, it can display
      # the appropriate buttons. In particular...
      #    - A job always gets the 'move' dropdown
      #    - A job always gets the 'track' flag
      #    - A failed job gets the 'retry' button
      #    - An incomplete job gets the 'cancel' button
      job = client.jobs[q.put(Qless::Job, {})]

      visit "/jobs/#{job.jid}"
      expect(page).to have_selector('i.icon-remove')
      expect(page).to_not have_selector('i.icon-repeat')
      expect(page).to have_selector('i.icon-flag')
      expect(page).to have_selector('i.caret')

      # Let's fail the job and that it has the repeat button
      q.pop.fail('foo', 'bar')

      visit "/jobs/#{job.jid}"
      expect(page).to have_selector('i.icon-remove')
      expect(page).to have_selector('i.icon-repeat')
      expect(page).to have_selector('i.icon-flag')
      expect(page).to have_selector('i.caret')

      # Now let's complete the job and see that it doesn't have
      # the cancel button
      job.requeue('testing')
      q.pop.complete

      visit "/jobs/#{job.jid}"
      expect(page).to_not have_selector('i.icon-remove.cancel-job')
      expect(page).to_not have_selector('i.icon-repeat')
      expect(page).to have_selector('i.icon-flag')
      expect(page).to have_selector('i.caret')
    end

    it 'can display tags and priorities for jobs' do
      visit "/jobs/#{q.put(Qless::Job, {})}"
      expect(page).to have_selector('input[placeholder="Pri 0"]')

      visit "/jobs/#{q.put(Qless::Job, {}, priority: 123)}"
      expect(page).to have_selector('input[placeholder="Pri 123"]')

      visit "/jobs/#{q.put(Qless::Job, {}, priority: -123)}"
      expect(page).to have_selector('input[placeholder="Pri -123"]')

      visit "/jobs/#{q.put(Qless::Job, {}, tags: %w{foo bar widget})}"
      %w{foo bar widget}.each do |tag|
        expect(page).to have_selector('.tag', text: tag)
      end
    end

    it 'can track a job', js: true do
      # Make sure the job doesn't appear as tracked first, then
      # click the 'track' button, and come back to verify that
      # it's now being tracked
      jid = q.put(Qless::Job, {})

      visit '/track'
      expect(page).to_not have_selector('a', text: /#{jid[0..5]}/i)

      visit "/jobs/#{jid}"
      first('i.icon-flag').click
      try_repeatedly { client.jobs[jid].tracked }

      visit '/track'
      expect(page).to have_selector('a', text: /#{jid[0..5]}/i)
    end

    it 'can move a job', js: true do
      # Let's put a job, pop it, complete it, and then move it
      # back into the testing queue
      jid = q.put(Qless::Job, {})
      q.pop.complete

      visit "/jobs/#{jid}"
      # Get the job, check that it's complete
      expect(client.jobs[jid].state).to eq('complete')
      first('i.caret').click
      first('a', text: 'testing').click

      try_repeatedly { client.jobs[jid].state == 'waiting' }

      visit "/jobs/#{jid}"

      # Now get the job again, check it's waiting
      expect(client.jobs[jid].state).to eq('waiting')
      expect(client.jobs[jid].queue_name).to eq('testing')
    end

    it 'can retry a single job', js: true do
      # Put, pop, and fail a job, and then click the retry button
      jid = q.put(Qless::Job, {})
      q.pop.fail('foo', 'bar')
      expect(client.jobs[jid].state).to eq('failed')

      visit "/jobs/#{jid}"

      # Retry the job
      first('i.icon-repeat').click

      try_repeatedly { client.jobs[jid].state == 'waiting' }

      job = client.jobs[jid]
      expect(job.state).to eq('waiting')
      expect(job.queue_name).to eq('testing')
    end

    it 'can cancel a single job', js: true do
      # Put, pop, and fail a job, and then click the retry button
      jid = q.put(Qless::Job, {})

      visit "/jobs/#{jid}"
      # Get the job, check that it's failed
      expect(client.jobs[jid].state).to eq('waiting')
      first('button.btn-danger').click
      # We should have to click the cancel button now
      expect(client.jobs[jid]).to be
      first('button.btn-danger').click

      try_repeatedly { client.jobs[jid].nil? }

      visit "/jobs/#{jid}"

      expect(page).to have_selector('h2', text: "#{jid} doesn't exist")
    end

    it 'can visit the configuration' do
      # Visit the bare-bones config page, make sure defaults are
      # present, then set some configurations, and then make sure
      # they appear as well
      visit '/config'
      expect(page).to have_selector('h2', text: /jobs-history-count/i)
      expect(page).to have_selector('h2', text: /stats-history/i)
      expect(page).to have_selector('h2', text: /jobs-history/i)

      client.config['foo-bar'] = 50
      visit '/config'
      expect(page).to have_selector('h2', text: /jobs-history-count/i)
      expect(page).to have_selector('h2', text: /stats-history/i)
      expect(page).to have_selector('h2', text: /jobs-history/i)
      expect(page).to have_selector('h2', text: /foo-bar/i)
    end

    it 'can search by tag' do
      # We should tag some jobs, and then search by tags and ensure
      # that we find all the jobs we'd expect
      foo    = 5.times.map { |i| q.put(Qless::Job, {}, tags: ['foo']) }
      bar    = 5.times.map { |i| q.put(Qless::Job, {}, tags: ['bar']) }
      foobar = 5.times.map { |i| q.put(Qless::Job, {}, tags: %w{foo bar}) }

      visit '/tag?tag=foo'
      (foo + foobar).each do |jid|
        expect(page).to have_selector('a', text: /#{jid[0..5]}/i)
      end

      visit '/tag?tag=bar'
      (bar + foobar).each do |jid|
        expect(page).to have_selector('a', text: /#{jid[0..5]}/i)
      end
    end

    it 'can visit the page for a specific job' do
      # We should make sure we see details like its state, the queue
      # that it's in, its data, and any failure information
      jid = q.put(Qless::Job, { foo: 'bar' })
      job = client.jobs[jid]

      visit "/jobs/#{jid}"
      # Make sure we see its klass_name, queue, state and data
      expect(page).to have_selector('h2', text: /#{job.klass}/i)
      expect(page).to have_selector('h2', text: /#{job.queue_name}/i)
      expect(page).to have_selector('h2', text: /#{job.state}/i)
      expect(page).to have_selector('pre', text: /\"foo\"\s*:\s*\"bar\"/im)

      # Now let's pop the job and fail it just to make sure we see the error
      q.pop.fail('something-something', 'what-what')

      visit "/jobs/#{jid}"
      expect(page).to have_selector('pre', text: /what-what/im)
    end

    it 'can visit the failed page' do
      # We should make sure that we see all the groups of failures that
      # we expect, as well as all the jobs we'd expect. This includes the
      # tabs, but also the section headings
      visit '/failed'
      expect(page).to_not have_selector('li', text: /foo/i)
      expect(page).to_not have_selector('li', text: /bar/i)

      foo = 5.times.map { |i| q.put(Qless::Job, {}) }
      q.pop(5).each { |job| job.fail('foo', 'foo-message') }
      bar = 5.times.map { |i| q.put(Qless::Job, {}) }
      q.pop(5).each { |job| job.fail('bar', 'bar-message') }

      try_repeatedly { client.jobs[bar.last].state == 'failed' }

      visit '/failed'
      expect(page).to have_selector('li', text: /foo\D+5/i)
      expect(page).to have_selector('li', text: /bar\D+5/i)
      expect(page).to have_selector('h2', text: /foo\D+5/i)
      expect(page).to have_selector('h2', text: /bar\D+5/i)
      (foo + bar).each do |jid|
        expect(page).to have_selector('a', text: /#{jid[0..5]}/i)
      end
    end

    it 'can visit the completed page' do
      foo = q.put(Qless::Job, {})
      bar = q.put(Qless::Job, {})

      visit '/completed'
      expect(page).to_not have_selector('a', :text => /#{foo[0..5]}/i)
      expect(page).to_not have_selector('a', :text => /#{bar[0..5]}/i)

      q.pop.complete
      q.pop.fail('foo', 'foo-message')

      visit '/completed'
      expect(page).to have_selector('a', :text => /#{foo[0..5]}/i)
      expect(page).to_not have_selector('a', :text => /#{bar[0..5]}/i)
    end

    it 'can retry a group of failed jobs', js: true do
      # We'll fail a bunch of jobs, with two kinds of errors,
      # and then we'll make sure that we can retry all of
      # one kind, but the rest still remain failed.
      foo = 5.times.map { |i| q.put(Qless::Job, {}) }
      q.pop(5).each { |job| job.fail('foo', 'foo-message') }
      bar = 5.times.map { |i| q.put(Qless::Job, {}) }
      q.pop(5).each { |job| job.fail('bar', 'bar-message') }

      try_repeatedly { client.jobs[bar.last].state == 'failed' }

      visit '/failed'
      expect(page).to have_selector('li', text: /foo\D+5/i)
      expect(page).to have_selector('h2', text: /foo\D+5/i)
      expect(page).to have_selector('li', text: /bar\D+5/i)
      expect(page).to have_selector('h2', text: /bar\D+5/i)
      (foo + bar).each do |jid|
        expect(page).to have_selector('a', text: /#{jid[0..5]}/i)
      end

      retry_button = (all('button').select do |b|
        !b['onclick'].nil? &&
         b['onclick'].include?('retryall') &&
         b['onclick'].include?('foo')
      end).first
      expect(retry_button).to be
      retry_button.click

      try_repeatedly { foo.all? { |jid| client.jobs[jid].state == 'waiting' } }

      # Now we shouldn't see any of those jobs, but we should
      # still see bar jobs
      visit '/failed'
      expect(page).to_not have_selector('li', text: /foo\D+5/i)
      expect(page).to_not have_selector('h2', text: /foo\D+5/i)
      expect(page).to have_selector('li', text: /bar\D+5/i)
      expect(page).to have_selector('h2', text: /bar\D+5/i)
      bar.each do |jid|
        expect(page).to have_selector('a', text: /#{jid[0..5]}/i)
      end
      foo.each do |jid|
        expect(page).to_not have_selector('a', text: /#{jid[0..5]}/i)
        expect(client.jobs[jid].state).to eq('waiting')
      end
    end

    it 'can cancel a group of failed jobs', js: true do
      # We'll fail a bunch of jobs, with two kinds of errors,
      # and then we'll make sure that we can retry all of
      # one kind, but the rest still remain failed.
      foo = 5.times.map { |i| q.put(Qless::Job, {}) }
      q.pop(5).each { |job| job.fail('foo', 'foo-message') }
      bar = 5.times.map { |i| q.put(Qless::Job, {}) }
      q.pop(5).each { |job| job.fail('bar', 'bar-message') }

      visit '/failed'
      expect(page).to have_selector('li', text: /foo\D+5/i)
      expect(page).to have_selector('h2', text: /foo\D+5/i)
      expect(page).to have_selector('li', text: /bar\D+5/i)
      expect(page).to have_selector('h2', text: /bar\D+5/i)
      (foo + bar).each do |jid|
        expect(page).to have_selector('a', text: /#{jid[0..5]}/i)
      end

      retry_button = (all('button').select do |b|
        !b['onclick'].nil? &&
         b['onclick'].include?('cancelall') &&
         b['onclick'].include?('foo')
      end).first
      expect(retry_button).to be
      retry_button.click
      # One click ain't gonna cut it
      foo.each do |jid|
        expect(client.jobs[jid].state).to eq('failed')
      end
      retry_button.click
      try_repeatedly { foo.all? { |jid| client.jobs[jid].nil? } }

      # Now we shouldn't see any of those jobs, but we should
      # still see bar jobs
      visit '/failed'
      expect(page).to_not have_selector('li', text: /foo\D+5/i)
      expect(page).to_not have_selector('h2', text: /foo\D+5/i)
      expect(page).to have_selector('li', text: /bar\D+5/i)
      expect(page).to have_selector('h2', text: /bar\D+5/i)
      bar.each do |jid|
        expect(page).to have_selector('a', text: /#{jid[0..5]}/i)
      end
      foo.each do |jid|
        expect(page).to_not have_selector('a', text: /#{jid[0..5]}/i)
        expect(client.jobs[jid]).to be_nil
      end
    end

    it 'can change a job\'s priority', js: true do
      jid = q.put(Qless::Job, {})

      visit "/jobs/#{jid}"
      expect(page).to_not have_selector('input[placeholder="Pri 25"]')
      expect(page).to have_selector('input[placeholder="Pri 0"]')
      first('input[placeholder="Pri 0"]').set(25)
      first('input[placeholder="Pri 0"]').trigger('blur')
      expect(page).to have_selector('input[placeholder="Pri 25"]')

      try_repeatedly { client.jobs[jid].priority == 25 }

      visit "/jobs/#{jid}"
      expect(page).to have_selector('input[placeholder="Pri 25"]')
      expect(page).to_not have_selector('input[placeholder="Pri 0"]')
    end

    it 'can add tags to a job', js: true do
      jid = q.put(Qless::Job, {})

      visit "/jobs/#{jid}"
      expect(page).to_not have_selector('.tag', text: 'foo')
      expect(page).to_not have_selector('.tag', text: 'bar')
      expect(page).to_not have_selector('.tag', text: 'whiz')
      first('input[placeholder="Add Tag"]').set('foo')
      first('input[placeholder="Add Tag"]').trigger('blur')
      try_repeatedly { client.jobs[jid].tags.length > 0 }

      visit "/jobs/#{jid}"
      expect(page).to have_selector('.tag', text: 'foo')
      expect(page).to_not have_selector('.tag', text: 'bar')
      expect(page).to_not have_selector('.tag', text: 'whiz')
      first('input[placeholder="Add Tag"]').set('bar')
      first('input[placeholder="Add Tag"]').trigger('blur')

      expect(page).to have_selector('.tag', text: 'foo')
      expect(page).to have_selector('.tag', text: 'bar')
      expect(page).to_not have_selector('.tag', text: 'whiz')
      first('input[placeholder="Add Tag"]').set('foo')
      first('input[placeholder="Add Tag"]').trigger('blur')
      try_repeatedly { client.jobs[jid].tags.length > 1 }

      # Now revisit the page and make sure it's happy
      visit("/jobs/#{jid}")
      expect(page).to have_selector('.tag', text: 'foo')
      expect(page).to have_selector('.tag', text: 'bar')
      expect(page).to_not have_selector('.tag', text: 'whiz')
    end

    it 'can remove tags', js: true do
      jid = q.put(Qless::Job, {}, tags: %w{foo bar})
      visit "/jobs/#{jid}"
      expect(page).to have_selector('.tag', text: 'foo')
      expect(page).to have_selector('.tag', text: 'bar')
      expect(page).to_not have_selector('.tag', text: 'whiz')

      first('.tag', :text => 'foo').sibling('button').click
      try_repeatedly { client.jobs[jid].tags.length == 1 }
      # Wait for it to disappear
      expect(page).to_not have_selector('.tag', text: 'foo')

      first('.tag', :text => 'bar').sibling('button').click
      # Wait for it to disappear
      try_repeatedly { client.jobs[jid].tags.length == 0 }
      expect(page).to_not have_selector('.tag', text: 'bar')
    end

    it 'can remove tags it has just added', js: true do
      jid = q.put(Qless::Job, {})
      visit "/jobs/#{jid}"
      expect(page).to_not have_selector('.tag', text: 'foo')
      expect(page).to_not have_selector('.tag', text: 'bar')
      expect(page).to_not have_selector('.tag', text: 'whiz')
      first('input[placeholder="Add Tag"]').set('foo')
      first('input[placeholder="Add Tag"]').trigger('blur')
      first('input[placeholder="Add Tag"]').set('bar')
      first('input[placeholder="Add Tag"]').trigger('blur')
      first('input[placeholder="Add Tag"]').set('whiz')
      first('input[placeholder="Add Tag"]').trigger('blur')

      first('.tag', text: 'foo').sibling('button').click
      try_repeatedly { client.jobs[jid].tags.length == 2 }
      expect(page).to have_no_selector('.tag', text: 'foo')

      first('.tag', text: 'bar').sibling('button').click
      try_repeatedly { client.jobs[jid].tags.length == 1 }
      expect(page).to have_no_selector('.tag', text: 'bar')

      first('.tag', text: 'whiz').sibling('button').click
      try_repeatedly { client.jobs[jid].tags.length == 0 }
      expect(page).to have_no_selector('.tag', text: 'whiz')
    end

    it 'can sort failed groupings by the number of affected jobs' do
      # Alright, let's make 10 different failure types, and then give them
      # a certain number of jobs each, and then make sure that they stay sorted
      %w{a b c d e f g h i j}.each_with_index do |group, index|
        (index + 5).times do |i|
          q.put(Qless::Job, {})
          q.pop.fail(group, 'testing')
        end
      end

      visit '/'
      groups = all(:xpath, '//a[contains(@href, "/failed/")]')
      expect(groups.map { |g| g.text }.join(' ')).to eq('j i h g f e d c b a')

      10.times do |i|
        q.put(Qless::Job, {})
        q.pop.fail('e', 'testing')
      end

      visit '/'
      groups = all(:xpath, '//a[contains(@href, "/failed/")]')
      expect(groups.map { |g| g.text }.join(' ')).to eq('e j i h g f d c b a')
    end

    it 'can visit /queues' do
      # We should be able to see all of the appropriate tabs,
      # We should be able to see all of the jobs
      q.put(Qless::Job, {})

      # We should see this job
      visit '/queues'
      expect(page).to have_selector('h3', text: /0\D+1\D+0\D+0\D+0\D+0\D+0/)

      # Now let's pop off the job so that it's running
      job = q.pop

      visit '/queues'
      expect(page).to have_selector('h3', text: /1\D+0\D+0\D+0\D+0\D+0\D+0/)
      job.complete

      # And now for a throttled job
      client.throttles['one'].maximum = 1
      q.put(Qless::Job, {}, throttles: ['one'])
      q.put(Qless::Job, {}, throttles: ['one'])
      job1, job2 = q.pop(2)

      visit '/queues'
      expect(page).to have_selector('h3', text: /1\D+0\D+1\D+0\D+0\D+0\D+0/)
      job1.complete
      q.pop.complete

      # And now for a scheduled job
      job = client.jobs[q.put(Qless::Job, {}, delay: 600)]

      visit '/queues'
      expect(page).to have_selector('h3', text: /0\D+0\D+0\D+1\D+0\D+0\D+0/)
      job.cancel

      # And now a dependent job
      job1 = client.jobs[q.put(Qless::Job, {})]
      job2 = client.jobs[q.put(Qless::Job, {}, depends: [job1.jid])]

      visit '/queues'
      expect(page).to have_selector('h3', text: /0\D+1\D+0\D+0\D+0\D+1\D+0/)
      job2.cancel
      job1.cancel

      # And now a recurring job
      job = client.jobs[q.recur(Qless::Job, {}, 5)]

      visit '/queues'
      expect(page).to have_selector('h3', text: /0\D+0\D+0\D+0\D+0\D+0\D+1/)
      job.cancel
    end

    it 'can visit the various /queues/* endpoints' do
      # We should be able to see all of the appropriate tabs,
      # We should be able to see all of the jobs
      jid = q.put(Qless::Job, {})

      # We should see this job
      visit '/queues/testing/waiting'
      expect(page).to have_selector('h2', text: /#{jid[0...8]}/)
      # Now let's pop off the job so that it's running
      job = q.pop

      visit '/queues/testing/running'
      expect(page).to have_selector('h2', text: /#{jid[0...8]}/)
      job.complete

      # And now for a throttled job
      client.throttles['one'].maximum = 1
      job1 = client.jobs[q.put(Qless::Job, {}, throttles: ['one'])]
      job2 = client.jobs[q.put(Qless::Job, {}, throttles: ['one'])]
      q.pop(2)

      visit '/queues/testing/throttled'
      expect(page).to have_selector('h2', text: /#{job2.jid[0...8]}/)
      job1.cancel
      job2.cancel

      # And now for a scheduled job
      job = client.jobs[q.put(Qless::Job, {}, delay: 600)]

      visit '/queues/testing/scheduled'
      expect(page).to have_selector('h2', text: /#{job.jid[0...8]}/)
      job.cancel

      # And now a dependent job
      job = client.jobs[q.put(Qless::Job, {}, depends: [q.put(Qless::Job, {})])]

      visit '/queues/testing/depends'
      expect(page).to have_selector('h2', text: /#{job.jid[0...8]}/)
      job.cancel

      # And now a recurring job
      job = client.jobs[q.recur(Qless::Job, {}, 5)]

      visit '/queues/testing/recurring'
      expect(page).to have_selector('h2', text: /#{job.jid[0...8]}/)
      job.cancel
    end

    it 'shows the state of tracked jobs in the overview' do
      # We should be able to see all of the appropriate tabs,
      # We should be able to see all of the jobs
      jid = q.put(Qless::Job, {})
      client.jobs[jid].track

      visit '/'
      # We should see it under 'waiting'
      expect(page).to have_selector('.tracked-row', text: /waiting/i)
      # Now let's pop off the job so that it's running
      job = q.pop

      visit '/'
      expect(page).to_not have_selector('.tracked-row', text: /waiting/i)
      expect(page).to have_selector('.tracked-row', text: /running/i)
      # Now let's complete the job and make sure it shows up again
      job.complete

      visit '/'
      expect(page).to_not have_selector('.tracked-row', text: /running/i)
      expect(page).to have_selector('.tracked-row', text: /complete/i)
      job.untrack
      expect(page).to have_selector('.tracked-row', text: /complete/i)

      # And now for a throttled job
      client.throttles['one'].maximum = 1
      client.jobs[q.put(Qless::Job, {}, throttles: ['one'])]
      job2 = client.jobs[q.put(Qless::Job, {}, throttles: ['one'])]
      job2.track
      q.pop(2)

      visit '/'
      expect(page).to have_selector('.tracked-row', text: /throttled/i)
      job2.untrack

      # And now for a scheduled job
      job = client.jobs[q.put(Qless::Job, {}, delay: 600)]
      job.track

      visit '/'
      expect(page).to have_selector('.tracked-row', text: /scheduled/i)
      job.untrack

      # And a failed job
      q.put(Qless::Job, {})
      job = q.pop
      job.track
      job.fail('foo', 'bar')

      visit '/'
      expect(page).to have_selector('.tracked-row', text: /failed/i)
      job.untrack

      # And a depends job
      job = client.jobs[
        q.put(Qless::Job, {}, depends: [q.put(Qless::Job, {})])]
      job.track

      visit '/'
      expect(page).to have_selector('.tracked-row', text: /depends/i)
      job.untrack
    end

    it 'can display, cancel, move recurring jobs', js: true do
      # We should create a recurring job and then make sure we can see it
      jid = q.recur(Qless::Job, {}, 600)

      visit "/jobs/#{jid}"
      expect(page).to have_selector('h2', text: jid[0...8])
      expect(page).to have_selector('h2', text: 'recurring')
      expect(page).to have_selector('h2', text: 'testing')
      expect(page).to have_selector('h2', text: 'Qless::Job')
      expect(page).to have_selector('button.btn-danger')
      expect(page).to have_selector('i.caret')

      # Cancel it
      first('button.btn-danger').click
      # We should have to click the cancel button now
      first('button.btn-danger').click
      try_repeatedly { client.jobs[jid].nil? }

      # Move it to another queue
      jid = other.recur(Qless::Job, {}, 600)
      expect(client.jobs[jid].queue_name).to eq('other')
      visit "/jobs/#{jid}"
      first('i.caret').click
      first('a', text: 'testing').click
      # Now get the job again, check it's waiting
      try_repeatedly { client.jobs[jid].queue_name == 'testing' }
      expect(client.jobs[jid].queue_name).to eq('testing')
    end

    it 'can change recurring job priorities', js: true do
      jid = q.recur(Qless::Job, {}, 600)

      visit "/jobs/#{jid}"
      expect(page).to_not have_selector('input[placeholder="Pri 25"]')
      expect(page).to have_selector('input[placeholder="Pri 0"]')
      first('input[placeholder="Pri 0"]').set(25)
      first('input[placeholder="Pri 0"]').trigger('blur')
      expect(page).to have_selector('input[placeholder="Pri 25"]')

      try_repeatedly { client.jobs[jid].priority == 25 }

      # And reload the page to make sure it's stuck between reloads
      visit "/jobs/#{jid}"
      expect(page).to have_selector('input[placeholder="Pri 25"]')
      expect(page).to_not have_selector('input[placeholder="Pri 0"]')
    end

    it 'can pause and unpause a queue', js: true do
      10.times { q.put(Qless::Job, {}) }
      visit '/'

      expect(q.pop).to be
      button = first('button[data-original-title="Pause"]')
      expect(button).to be
      button.click
      try_repeatedly { q.paused? }
      expect(page).to have_selector('button[data-original-title="Unpause"]')
      expect(q.pop).to_not be

      first('button[data-original-title="Unpause"]').click
      try_repeatedly { !q.paused? }
      expect(q.pop).to be
    end

    it 'can add tags to a recurring job', js: true do
      jid = q.put(Qless::Job, {})

      visit "/jobs/#{jid}"
      expect(page).to_not have_selector('.tag', text: 'foo')
      expect(page).to_not have_selector('.tag', text: 'bar')
      expect(page).to_not have_selector('.tag', text: 'whiz')
      first('input[placeholder="Add Tag"]').set('foo')
      first('input[placeholder="Add Tag"]').trigger('blur')

      expect(page).to have_selector('.tag', text: 'foo')
      expect(page).to_not have_selector('.tag', text: 'bar')
      expect(page).to_not have_selector('.tag', text: 'whiz')
      first('input[placeholder="Add Tag"]').set('bar')
      first('input[placeholder="Add Tag"]').trigger('blur')

      expect(page).to have_selector('.tag', text: 'foo')
      expect(page).to have_selector('.tag', text: 'bar')
      expect(page).to_not have_selector('.tag', text: 'whiz')
      first('input[placeholder="Add Tag"]').set('foo')
      first('input[placeholder="Add Tag"]').trigger('blur')

      try_repeatedly { client.jobs[jid].tags.length == 2 }

      # Now revisit the page and make sure it's happy
      visit("/jobs/#{jid}")
      expect(page).to have_selector('.tag', text: 'foo')
      expect(page).to have_selector('.tag', text: 'bar')
      expect(page).to_not have_selector('.tag', text: 'whiz')
    end
  end

  describe 'Rack Tests', :integration do
    include Rack::Test::Methods

    # Our main test queue
    let(:q)      { client.queues['testing'] }
    let(:app)    { Qless::Server.new(Qless::Client.new(redis_config)) }

    it 'can access the JSON endpoints for queue sizes' do
      q.put(Qless::Job, {})
      get '/queues.json'
      response = {
          'running'   => 0,
          'name'      => 'testing',
          'waiting'   => 1,
          'recurring' => 0,
          'depends'   => 0,
          'stalled'   => 0,
          'scheduled' => 0,
          'throttled' => 0,
          'paused'    => false
        }
      expect(JSON.parse(last_response.body)).to eq([response])

      get '/queues/testing.json'
      expect(JSON.parse(last_response.body)).to eq(response)
    end

    it 'can access the JSON endpoint for failures' do
      get '/failed.json'
      expect(JSON.parse(last_response.body)).to eq({})

      # Now, put a job in, pop it and fail it, make sure we see
      q.put(Qless::Job, {})
      job = q.pop
      job.fail('foo', 'bar')
      get '/failed.json'
      expect(JSON.parse(last_response.body)).to eq({ 'foo' => 1 })
    end
  end
end
