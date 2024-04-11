# Encoding: utf-8

ENV['RACK_ENV'] = 'test'
require 'spec_helper'
require 'yaml'
require 'qless'
require 'qless/server'
require 'capybara/rspec'
require 'capybara/poltergeist'
require 'rack/test'
require 'pry'

Capybara.javascript_driver = :poltergeist

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

      links = all('ul.nav a')
      expect(links).to have_at_least(7).links
      links.each do |link|
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
      within '.pagination' do
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

    it 'can paginate jobs on a state filter tab' do
      q.put(Qless::Job, {}, jid: 'parent-job')

      build_paginated_objects do |jid|
        q.put(Qless::Job, {}, jid: jid, depends: ['parent-job'])
      end

      visit "/queues/#{CGI.escape(q.name)}/depends"

      test_pagination
    end

    it 'can set and delete queues throttles', js: true do
      q.put(Qless::Job, {})

      text_field_class = ".ql-q-#{q.name}-maximum"

      expect(q.throttle.maximum).to eq(0)

      visit '/throttles'

      expect(first('td', text: /ql:q:#{q.name}/i)).to be
      expect(first(text_field_class, placeholder: /0/i)).to be

      maximum = first(text_field_class)
      maximum.set(3)
      maximum.trigger('blur')

      expect(first(text_field_class, value: /3/i)).to be
      expect(q.throttle.maximum).to eq(3)

      first('button.btn-danger').click
      first('button.btn-danger').click

      expect(first(text_field_class, value: /0/i)).to be
    end

    it 'can set the expiration for queue throttles', js: true do
      q.put(Qless::Job, {})

      maximum_field_class = ".ql-q-#{q.name}-maximum"
      expiration_field_class = ".ql-q-#{q.name}-expiration"

      expect(q.throttle.maximum).to eq(0)
      expect(q.throttle.ttl).to eq(-2)

      visit '/throttles'

      expect(first('td', text: /ql:q:#{q.name}/i)).to be
      expect(first(expiration_field_class, placeholder: /-2/i)).to be

      maximum = first(maximum_field_class)
      maximum.set(3)
      maximum.trigger('blur')

      expect(first(maximum_field_class, value: /3/i)).to be
      expect(q.throttle.maximum).to eq(3)

      expiration = first(expiration_field_class)
      expiration.set(1)
      expiration.trigger('blur')

      visit '/throttles'

      expect(first(maximum_field_class, value: /0/i)).to be
      expect(first(expiration_field_class, placeholder: /-2/i)).to be
    end

    it 'can set and delete job throttles', js: true do
      t_id = 'wakka' # the throttle id
      jid = q.put(Qless::Job, {}, throttles: [t_id])

      text_field_class = ".#{t_id}-maximum"
      throttle = Throttle.new(t_id, client)

      expect(throttle.maximum).to eq(0)

      visit "/jobs/#{jid}"

      expect(page).to have_content(t_id)
      expect(first(".#{t_id}-maximum", placeholder: /0/i)).to be

      maximum = first(".#{t_id}-maximum")
      maximum.set(3)
      maximum.trigger('blur')

      expect(first(".#{t_id}-maximum", value: /3/i)).to be
      expect(throttle.maximum).to eq(3)

      first('button.btn-danger.remove-throttle').click
      first('button.btn-danger.remove-throttle').click

      expect(first(".#{t_id}-maximum", value: /0/i)).to be
    end

    it 'can set the expiration for job throttles', js: true do
      t_id = 'wakka' # the throttle id
      jid = q.put(Qless::Job, {}, throttles: [t_id])

      maximum_field_class = ".#{t_id}-maximum"
      expiration_field_class = ".#{t_id}-expiration"
      throttle = Throttle.new(t_id, client)

      expect(throttle.maximum).to eq(0)
      expect(throttle.ttl).to eq(-2)

      visit "/jobs/#{jid}"

      expect(page).to have_content(t_id)
      expect(first(".#{t_id}-expiration", placeholder: /-2/i)).to be

      maximum = first(".#{t_id}-maximum")
      maximum.set(3)
      maximum.trigger('blur')

      expect(first(".#{t_id}-maximum", value: /3/i)).to be
      expect(throttle.maximum).to eq(3)

      expiration = first(".#{t_id}-expiration")
      expiration.set(1)
      expiration.trigger('blur')

      visit "/jobs/#{jid}"

      expect(first(".#{t_id}-maximum", value: /0/i)).to be
      expect(first(".#{t_id}-expiration", placeholder: /-2/i)).to be
    end

    it 'can see the root-level summary' do
      visit '/'

      # When qless is empty, we should see the default bit
      expect(first('h1', text: /no queues/i)).to be
      expect(first('h1', text: /no failed jobs/i)).to be
      expect(first('h1', text: /no workers/i)).to be

      # Alright, let's add a job to a queue, and make sure we can see it
      q.put(Qless::Job, {})
      visit '/'
      expect(first('.queue-row', text: /testing/)).to be
      expect(first('.queue-row', text: /0\D+1\D+0\D+0\D+0\D+0\D+0/)).to be
      expect(first('h1', text: /no queues/i)).to be_nil
      expect(first('h1', text: /queues and their job counts/i)).to be

      # Let's pop the job, and make sure that we can see /that/
      job = q.pop
      visit '/'
      expect(first('.queue-row', text: /1\D+0\D+0\D+0\D+0\D+0\D+0/)).to be
      expect(first('.worker-row', text: q.worker_name)).to be
      expect(first('.worker-row', text: /1\D+0/i)).to be

      # Let's complete the job, and make sure it disappears
      job.complete
      visit '/'
      expect(first('.queue-row', text: /0\D+0\D+0\D+0\D+0\D+0\D+0/)).to be
      expect(first('.worker-row', text: /0\D+0/i)).to be

      # Let's throttle a job, and make sure we see it
      client.throttles['one'].maximum = 1
      q.put(Qless::Job, {}, :throttles => ["one"])
      q.put(Qless::Job, {}, :throttles => ["one"])
      job1 = q.pop
      job2 = q.pop
      visit '/'
      expect(first('.queue-row', text: /1\D+0\D+1\D+0\D+0\D+0\D+0/)).to be
      job1.complete
      q.pop.complete

      # Let's put and pop and fail a job, and make sure we see it
      q.put(Qless::Job, {})
      job = q.pop
      job.fail('foo-failure', 'bar')
      visit '/'
      expect(first('.failed-row', text: /foo-failure/)).to be
      expect(first('.failed-row', text: /1/i)).to be

      # And let's have one scheduled, and make sure it shows up accordingly
      jid = q.put(Qless::Job, {}, delay: 60)
      visit '/'
      expect(first('.queue-row', text: /0\D+0\D+0\D+1\D+0\D+0\D+0/)).to be
      # And one that depends on that job
      q.put(Qless::Job, {}, depends: [jid])
      visit '/'
      expect(first('.queue-row', text: /0\D+0\D+0\D+1\D+0\D+1\D+0/)).to be
    end

    it 'can visit the tracked page' do
      # We should be able to see all of the appropriate tabs,
      # We should be able to see all of the jobs
      jid = q.put(Qless::Job, {})
      client.jobs[jid].track

      visit '/track'
      # Make sure it appears under 'all', and 'waiting'
      expect(first('a', text: /all\W+1/i)).to be
      expect(first('a', text: /waiting\W+1/i)).to be
      # Now let's pop off the job so that it's running
      job = q.pop
      visit '/track'
      expect(first('a', text: /all\W+1/i)).to be
      expect(first('a', text: /waiting\W+0/i)).to be
      expect(first('a', text: /running\W+1/i)).to be
      # Now let's complete the job and make sure it shows up again
      job.complete
      visit '/track'
      expect(first('a', text: /all\W+1/i)).to be
      expect(first('a', text: /running\W+0/i)).to be
      expect(first('a', text: /completed\W+1/i)).to be
      job.untrack

      # And now for a throttled job
      client.throttles['one'].maximum = 1
      q.put(Qless::Job, {}, throttles: ["one"])
      job = client.jobs[q.put(Qless::Job, {}, throttles: ["one"])]
      job.track
      q.pop(2)
      visit '/track'
      expect(first('a', text: /all\W+1/i)).to be
      expect(first('a', text: /throttled\W+1/i)).to be
      job.untrack

      # And now for a scheduled job
      job = client.jobs[q.put(Qless::Job, {}, delay: 600)]
      job.track
      visit '/track'
      expect(first('a', text: /all\W+1/i)).to be
      expect(first('a', text: /scheduled\W+1/i)).to be
      job.untrack

      # And a failed job
      q.put(Qless::Job, {})
      job = q.pop
      job.track
      job.fail('foo', 'bar')
      visit '/track'
      expect(first('a', text: /all\W+1/i)).to be
      expect(first('a', text: /failed\W+1/i)).to be
      job.untrack

      # And a depends job
      job = client.jobs[
        q.put(Qless::Job, {}, depends: [q.put(Qless::Job, {})])]
      job.track
      visit '/track'
      expect(first('a', text: /all\W+1/i)).to be
      expect(first('a', text: /depends\W+1/i)).to be
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
      expect(first('i.icon-remove')).to be
      expect(first('i.icon-repeat')).to be_nil
      expect(first('i.icon-flag')).to be
      expect(first('i.caret')).to be

      # Let's fail the job and that it has the repeat button
      q.pop.fail('foo', 'bar')
      visit "/jobs/#{job.jid}"
      expect(first('i.icon-remove')).to be
      expect(first('i.icon-repeat')).to be
      expect(first('i.icon-flag')).to be
      expect(first('i.caret')).to be

      # Now let's complete the job and see that it doesn't have
      # the cancel button
      job.requeue('testing')
      q.pop.complete
      visit "/jobs/#{job.jid}"
      expect(first('i.icon-remove.cancel-job')).to be_nil
      expect(first('i.icon-repeat')).to be_nil
      expect(first('i.icon-flag')).to be
      expect(first('i.caret')).to be
    end

    it 'can display tags and priorities for jobs' do
      visit "/jobs/#{q.put(Qless::Job, {})}"
      expect(first('input[placeholder="Pri 0"]')).to be

      visit "/jobs/#{q.put(Qless::Job, {}, priority: 123)}"
      expect(first('input[placeholder="Pri 123"]')).to be

      visit "/jobs/#{q.put(Qless::Job, {}, priority: -123)}"
      expect(first('input[placeholder="Pri -123"]')).to be

      visit "/jobs/#{q.put(Qless::Job, {}, tags: %w{foo bar widget})}"
      %w{foo bar widget}.each do |tag|
        expect(first('span', text: tag)).to be
      end
    end

    it 'can track a job', js: true do
      # Make sure the job doesn't appear as tracked first, then
      # click the 'track' button, and come back to verify that
      # it's now being tracked
      jid = q.put(Qless::Job, {})
      visit '/track'
      expect(first('a', text: /#{jid[0..5]}/i)).to be_nil
      visit "/jobs/#{jid}"
      first('i.icon-flag').click
      visit '/track'
      expect(first('a', text: /#{jid[0..5]}/i)).to be
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

      # Reload the page to synchronize and ensure the AJAX request completes.
      visit "/jobs/#{jid}"

      # Now get the job again, check it's waiting
      expect(client.jobs[jid].state).to eq('waiting')
      expect(client.jobs[jid].queue_name).to eq('testing')
    end

    it 'can retry a single job', js: true do
      # Put, pop, and fail a job, and then click the retry button
      jid = q.put(Qless::Job, {})
      q.pop.fail('foo', 'bar')
      visit "/jobs/#{jid}"
      # Get the job, check that it's failed
      expect(client.jobs[jid].state).to eq('failed')
      first('i.icon-repeat').click

      # Reload the page to synchronize and ensure the AJAX request completes.
      visit "/jobs/#{jid}"

      # Now get hte jobs again, check that it's waiting
      expect(client.jobs[jid].state).to eq('waiting')
      expect(client.jobs[jid].queue_name).to eq('testing')
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

      # Reload the page to synchronize and ensure the AJAX request completes.
      visit "/jobs/#{jid}"

      # /Now/ the job should be canceled
      expect(client.jobs[jid]).to be_nil
    end

    it 'can visit the configuration' do
      # Visit the bare-bones config page, make sure defaults are
      # present, then set some configurations, and then make sure
      # they appear as well
      visit '/config'
      expect(first('h2', text: /jobs-history-count/i)).to be
      expect(first('h2', text: /stats-history/i)).to be
      expect(first('h2', text: /jobs-history/i)).to be

      client.config['foo-bar'] = 50
      visit '/config'
      expect(first('h2', text: /jobs-history-count/i)).to be
      expect(first('h2', text: /stats-history/i)).to be
      expect(first('h2', text: /jobs-history/i)).to be
      expect(first('h2', text: /foo-bar/i)).to be
    end

    it 'can search by tag' do
      # We should tag some jobs, and then search by tags and ensure
      # that we find all the jobs we'd expect
      foo    = 5.times.map { |i| q.put(Qless::Job, {}, tags: ['foo']) }
      bar    = 5.times.map { |i| q.put(Qless::Job, {}, tags: ['bar']) }
      foobar = 5.times.map { |i| q.put(Qless::Job, {}, tags: %w{foo bar}) }

      visit '/tag?tag=foo'
      (foo + foobar).each do |jid|
        expect(first('a', text: /#{jid[0..5]}/i)).to be
      end

      visit '/tag?tag=bar'
      (bar + foobar).each do |jid|
        expect(first('a', text: /#{jid[0..5]}/i)).to be
      end
    end

    it 'can visit the page for a specific job' do
      # We should make sure we see details like its state, the queue
      # that it's in, its data, and any failure information
      jid = q.put(Qless::Job, { foo: 'bar' })
      job = client.jobs[jid]
      visit "/jobs/#{jid}"
      # Make sure we see its klass_name, queue, state and data
      expect(first('h2', text: /#{job.klass}/i)).to be
      expect(first('h2', text: /#{job.queue_name}/i)).to be
      expect(first('h2', text: /#{job.state}/i)).to be
      expect(first('pre', text: /\"foo\"\s*:\s*\"bar\"/im)).to be

      # Now let's pop the job and fail it just to make sure we see the error
      q.pop.fail('something-something', 'what-what')
      visit "/jobs/#{jid}"
      expect(first('pre', text: /what-what/im)).to be
    end

    it 'can visit the failed page' do
      # We should make sure that we see all the groups of failures that
      # we expect, as well as all the jobs we'd expect. This includes the
      # tabs, but also the section headings
      visit '/failed'
      expect(first('li', text: /foo/i)).to be_nil
      expect(first('li', text: /bar/i)).to be_nil

      foo = 5.times.map { |i| q.put(Qless::Job, {}) }
      q.pop(5).each { |job| job.fail('foo', 'foo-message') }
      bar = 5.times.map { |i| q.put(Qless::Job, {}) }
      q.pop(5).each { |job| job.fail('bar', 'bar-message') }
      visit '/failed'
      expect(first('li', text: /foo\D+5/i)).to be
      expect(first('li', text: /bar\D+5/i)).to be
      expect(first('h2', text: /foo\D+5/i)).to be
      expect(first('h2', text: /bar\D+5/i)).to be
      (foo + bar).each do |jid|
        expect(first('a', text: /#{jid[0..5]}/i)).to be
      end
    end

    it 'can visit the completed page' do
      foo = q.put(Qless::Job, {})
      bar = q.put(Qless::Job, {})

      visit '/completed'
      expect(first('a', :text => /#{foo[0..5]}/i)).to be_nil
      expect(first('a', :text => /#{bar[0..5]}/i)).to be_nil

      q.pop.complete
      q.pop.fail('foo', 'foo-message')

      visit '/completed'
      expect(first('a', :text => /#{foo[0..5]}/i)).to be
      expect(first('a', :text => /#{bar[0..5]}/i)).to be_nil
    end

    it 'can retry a group of failed jobs', js: true do
      # We'll fail a bunch of jobs, with two kinds of errors,
      # and then we'll make sure that we can retry all of
      # one kind, but the rest still remain failed.
      foo = 5.times.map { |i| q.put(Qless::Job, {}) }
      q.pop(5).each { |job| job.fail('foo', 'foo-message') }
      bar = 5.times.map { |i| q.put(Qless::Job, {}) }
      q.pop(5).each { |job| job.fail('bar', 'bar-message') }

      visit '/failed'
      expect(first('li', text: /foo\D+5/i)).to be
      expect(first('h2', text: /foo\D+5/i)).to be
      expect(first('li', text: /bar\D+5/i)).to be
      expect(first('h2', text: /bar\D+5/i)).to be
      (foo + bar).each do |jid|
        expect(first('a', text: /#{jid[0..5]}/i)).to be
      end

      retry_button = (all('button').select do |b|
        !b['onclick'].nil? &&
         b['onclick'].include?('retryall') &&
         b['onclick'].include?('foo')
      end).first
      expect(retry_button).to be
      retry_button.click

      # Now we shouldn't see any of those jobs, but we should
      # still see bar jobs
      visit '/failed'
      expect(first('li', text: /foo\D+5/i)).to be_nil
      expect(first('h2', text: /foo\D+5/i)).to be_nil
      expect(first('li', text: /bar\D+5/i)).to be
      expect(first('h2', text: /bar\D+5/i)).to be
      bar.each do |jid|
        expect(first('a', text: /#{jid[0..5]}/i)).to be
      end
      foo.each do |jid|
        expect(first('a', text: /#{jid[0..5]}/i)).to be_nil
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
      expect(first('li', text: /foo\D+5/i)).to be
      expect(first('h2', text: /foo\D+5/i)).to be
      expect(first('li', text: /bar\D+5/i)).to be
      expect(first('h2', text: /bar\D+5/i)).to be
      (foo + bar).each do |jid|
        expect(first('a', text: /#{jid[0..5]}/i)).to be
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

      # Now we shouldn't see any of those jobs, but we should
      # still see bar jobs
      visit '/failed'
      expect(first('li', text: /foo\D+5/i)).to be_nil
      expect(first('h2', text: /foo\D+5/i)).to be_nil
      expect(first('li', text: /bar\D+5/i)).to be
      expect(first('h2', text: /bar\D+5/i)).to be
      bar.each do |jid|
        expect(first('a', text: /#{jid[0..5]}/i)).to be
      end
      foo.each do |jid|
        expect(first('a', text: /#{jid[0..5]}/i)).to be_nil
        expect(client.jobs[jid]).to be_nil
      end
    end

    it 'can change a job\'s priority', js: true do
      jid = q.put(Qless::Job, {})
      visit "/jobs/#{jid}"
      expect(first('input[placeholder="Pri 25"]')).to_not be
      expect(first('input[placeholder="Pri 0"]')).to be
      first('input[placeholder="Pri 0"]').set(25)
      first('input[placeholder="Pri 0"]').trigger('blur')

      # Now, we should make sure that the placeholder's updated,
      expect(find('input[placeholder="Pri 25"]')).to be

      # And reload the page to make sure it's stuck between reloads
      visit "/jobs/#{jid}"
      expect(first('input[placeholder="Pri 25"]', placeholder: /\D*25/)).to be
      expect(first('input[placeholder="Pri 0"]', placeholder: /\D*0/)).to_not be
    end

    it 'can add tags to a job', js: true do
      jid = q.put(Qless::Job, {})
      visit "/jobs/#{jid}"
      expect(first('span', text: 'foo')).to_not be
      expect(first('span', text: 'bar')).to_not be
      expect(first('span', text: 'whiz')).to_not be
      first('input[placeholder="Add Tag"]').set('foo')
      first('input[placeholder="Add Tag"]').trigger('blur')

      visit "/jobs/#{jid}"
      expect(first('span', text: 'foo')).to be
      expect(first('span', text: 'bar')).to_not be
      expect(first('span', text: 'whiz')).to_not be
      first('input[placeholder="Add Tag"]').set('bar')
      first('input[placeholder="Add Tag"]').trigger('blur')

      expect(first('span', text: 'foo')).to be
      expect(first('span', text: 'bar')).to be
      expect(first('span', text: 'whiz')).to_not be
      first('input[placeholder="Add Tag"]').set('foo')
      first('input[placeholder="Add Tag"]').trigger('blur')

      # Now revisit the page and make sure it's happy
      visit("/jobs/#{jid}")
      expect(first('span', text: 'foo')).to be
      expect(first('span', text: 'bar')).to be
      expect(first('span', text: 'whiz')).to_not be
    end

    it 'can remove tags', js: true do
      jid = q.put(Qless::Job, {}, tags: %w{foo bar})
      visit "/jobs/#{jid}"
      expect(first('span', text: 'foo')).to be
      expect(first('span', text: 'bar')).to be
      expect(first('span', text: 'whiz')).to_not be

      # This appears to be selenium-only, but :contains works for what we need
      first('span:contains("foo") + button').click
      # Wait for it to disappear
      expect(first('span', text: 'foo')).to_not be

      first('span:contains("bar") + button').click
      # Wait for it to disappear
      expect(first('span', text: 'bar')).to_not be
    end

    it 'can remove tags it has just added', js: true do
      jid = q.put(Qless::Job, {})
      visit "/jobs/#{jid}"
      expect(first('span', text: 'foo')).to_not be
      expect(first('span', text: 'bar')).to_not be
      expect(first('span', text: 'whiz')).to_not be
      first('input[placeholder="Add Tag"]').set('foo')
      first('input[placeholder="Add Tag"]').trigger('blur')
      first('input[placeholder="Add Tag"]').set('bar')
      first('input[placeholder="Add Tag"]').trigger('blur')
      first('input[placeholder="Add Tag"]').set('whiz')
      first('input[placeholder="Add Tag"]').trigger('blur')

      # This appears to be selenium-only, but :contains works for what we need
      expect(first('span:contains("foo") + button')).to be
      first('span:contains("foo") + button').click
      # Wait for it to disappear
      expect(first('span', text: 'foo')).to_not be

      expect(first('span:contains("bar") + button')).to be
      first('span:contains("bar") + button').click
      # Wait for it to disappear
      expect(first('span', text: 'bar')).to_not be

      expect(first('span:contains("whiz") + button')).to be
      first('span:contains("whiz") + button').click
      # Wait for it to disappear
      expect(first('span', text: 'whiz')).to_not be
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
      jid = q.put(Qless::Job, {})

      # We should see this job
      visit '/queues'
      expect(first('h3', text: /0\D+1\D+0\D+0\D+0\D+0\D+0/)).to be

      # Now let's pop off the job so that it's running
      job = q.pop
      visit '/queues'
      expect(first('h3', text: /1\D+0\D+0\D+0\D+0\D+0\D+0/)).to be
      job.complete

      # And now for a throttled job
      client.throttles['one'].maximum = 1
      q.put(Qless::Job, {}, throttles: ["one"])
      q.put(Qless::Job, {}, throttles: ["one"])
      job1, job2 = q.pop(2)
      visit '/queues'
      expect(first('h3', text: /1\D+0\D+1\D+0\D+0\D+0\D+0/)).to be
      job1.complete
      q.pop.complete

      # And now for a scheduled job
      job = client.jobs[q.put(Qless::Job, {}, delay: 600)]
      visit '/queues'
      expect(first('h3', text: /0\D+0\D+0\D+1\D+0\D+0\D+0/)).to be
      job.cancel

      # And now a dependent job
      job1 = client.jobs[q.put(Qless::Job, {})]
      job2 = client.jobs[q.put(Qless::Job, {}, depends: [job1.jid])]
      visit '/queues'
      expect(first('h3', text: /0\D+1\D+0\D+0\D+0\D+1\D+0/)).to be
      job2.cancel
      job1.cancel

      # And now a recurring job
      job = client.jobs[q.recur(Qless::Job, {}, 5)]
      visit '/queues'
      expect(first('h3', text: /0\D+0\D+0\D+0\D+0\D+0\D+1/)).to be
      job.cancel
    end

    it 'can visit the various /queues/* endpoints' do
      # We should be able to see all of the appropriate tabs,
      # We should be able to see all of the jobs
      jid = q.put(Qless::Job, {})

      # We should see this job
      visit '/queues/testing/waiting'
      expect(first('h2', text: /#{jid[0...8]}/)).to be
      # Now let's pop off the job so that it's running
      job = q.pop
      visit '/queues/testing/running'
      expect(first('h2', text: /#{jid[0...8]}/)).to be
      job.complete

      # And now for a throttled job
      client.throttles['one'].maximum = 1
      job1 = client.jobs[q.put(Qless::Job, {}, throttles: ["one"])]
      job2 = client.jobs[q.put(Qless::Job, {}, throttles: ["one"])]
      q.pop(2)
      visit '/queues/testing/throttled'
      expect(first('h2', text: /#{job2.jid[0...8]}/)).to be
      job1.cancel
      job2.cancel

      # And now for a scheduled job
      job = client.jobs[q.put(Qless::Job, {}, delay: 600)]
      visit '/queues/testing/scheduled'
      expect(first('h2', text: /#{job.jid[0...8]}/)).to be
      job.cancel

      # And now a dependent job
      job = client.jobs[
        q.put(Qless::Job, {}, depends: [q.put(Qless::Job, {})])]
      visit '/queues/testing/depends'
      expect(first('h2', text: /#{job.jid[0...8]}/)).to be
      job.cancel

      # And now a recurring job
      job = client.jobs[q.recur(Qless::Job, {}, 5)]
      visit '/queues/testing/recurring'
      expect(first('h2', text: /#{job.jid[0...8]}/)).to be
      job.cancel
    end

    it 'shows the state of tracked jobs in the overview' do
      # We should be able to see all of the appropriate tabs,
      # We should be able to see all of the jobs
      jid = q.put(Qless::Job, {})
      client.jobs[jid].track

      visit '/'
      # We should see it under 'waiting'
      expect(first('.tracked-row', text: /waiting/i)).to be
      # Now let's pop off the job so that it's running
      job = q.pop
      visit '/'
      expect(first('.tracked-row', text: /waiting/i)).to_not be
      expect(first('.tracked-row', text: /running/i)).to be
      # Now let's complete the job and make sure it shows up again
      job.complete
      visit '/'
      expect(first('.tracked-row', text: /running/i)).to_not be
      expect(first('.tracked-row', text: /complete/i)).to be
      job.untrack
      expect(first('.tracked-row', text: /complete/i)).to be

      # And now for a throttled job
      client.throttles['one'].maximum = 1
      job1 = client.jobs[q.put(Qless::Job, {}, throttles: ["one"])]
      job2 = client.jobs[q.put(Qless::Job, {}, throttles: ["one"])]
      job2.track
      q.pop(2)
      visit '/'
      expect(first('.tracked-row', text: /throttled/i)).to be
      job2.untrack

      # And now for a scheduled job
      job = client.jobs[q.put(Qless::Job, {}, delay: 600)]
      job.track
      visit '/'
      expect(first('.tracked-row', text: /scheduled/i)).to be
      job.untrack

      # And a failed job
      q.put(Qless::Job, {})
      job = q.pop
      job.track
      job.fail('foo', 'bar')
      visit '/'
      expect(first('.tracked-row', text: /failed/i)).to be
      job.untrack

      # And a depends job
      job = client.jobs[
        q.put(Qless::Job, {}, depends: [q.put(Qless::Job, {})])]
      job.track
      visit '/'
      expect(first('.tracked-row', text: /depends/i)).to be
      job.untrack
    end

    it 'can display, cancel, move recurring jobs', js: true do
      # We should create a recurring job and then make sure we can see it
      jid = q.recur(Qless::Job, {}, 600)

      visit "/jobs/#{jid}"
      expect(first('h2', text: jid[0...8])).to be
      expect(first('h2', text: 'recurring')).to be
      expect(first('h2', text: 'testing')).to be
      expect(first('h2', text: 'Qless::Job')).to be
      expect(first('button.btn-danger')).to be
      expect(first('i.caret')).to be

      # Cancel it
      first('button.btn-danger').click
      # We should have to click the cancel button now
      first('button.btn-danger').click
      expect(client.jobs[jid]).to_not be

      # Move it to another queue
      jid = other.recur(Qless::Job, {}, 600)
      expect(client.jobs[jid].queue_name).to eq('other')
      visit "/jobs/#{jid}"
      first('i.caret').click
      first('a', text: 'testing').click
      # Now get the job again, check it's waiting
      expect(client.jobs[jid].queue_name).to eq('testing')
    end

    it 'can change recurring job priorities', js: true do
      jid = q.recur(Qless::Job, {}, 600)
      visit "/jobs/#{jid}"
      expect(first('input[placeholder="Pri 25"]')).to_not be
      expect(first('input[placeholder="Pri 0"]')).to be
      first('input[placeholder="Pri 0"]').set(25)
      first('input[placeholder="Pri 0"]').trigger('blur')

      # Now, we should make sure that the placeholder's updated,
      expect(find('input[placeholder="Pri 25"]')).to be

      # And reload the page to make sure it's stuck between reloads
      visit "/jobs/#{jid}"
      expect(first('input[placeholder="Pri 25"]', placeholder: /\D*25/)).to be
      expect(first('input[placeholder="Pri 0"]', placeholder: /\D*0/)).to_not be
    end

    it 'can pause and unpause a queue', js: true do
      10.times do
        q.put(Qless::Job, {})
      end
      visit '/'

      expect(q.pop).to be
      button = first('button', title: /Pause/)
      expect(button).to be
      button.click
      expect(q.pop).to_not be

      # Now we should unpause it
      first('button', title: /Unpause/).click
      expect(q.pop).to be
    end

    it 'can add tags to a recurring job', js: true do
      jid = q.put(Qless::Job, {})
      visit "/jobs/#{jid}"
      expect(first('span', text: 'foo')).to_not be
      expect(first('span', text: 'bar')).to_not be
      expect(first('span', text: 'whiz')).to_not be
      first('input[placeholder="Add Tag"]').set('foo')
      first('input[placeholder="Add Tag"]').trigger('blur')

      expect(find('span', text: 'foo')).to be
      expect(first('span', text: 'bar')).to_not be
      expect(first('span', text: 'whiz')).to_not be
      first('input[placeholder="Add Tag"]').set('bar')
      first('input[placeholder="Add Tag"]').trigger('blur')

      expect(find('span', text: 'foo')).to be
      expect(find('span', text: 'bar')).to be
      expect(first('span', text: 'whiz')).to_not be
      first('input[placeholder="Add Tag"]').set('foo')
      first('input[placeholder="Add Tag"]').trigger('blur')

      # Now revisit the page and make sure it's happy
      visit("/jobs/#{jid}")
      expect(find('span', text: 'foo')).to be
      expect(find('span', text: 'bar')).to be
      expect(first('span', text: 'whiz')).to_not be
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
