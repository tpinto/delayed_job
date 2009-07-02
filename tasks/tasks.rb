# Re-definitions are appended to existing tasks
task :environment
task :merb_env

namespace :jobs do
  desc "Clear the delayed_job queue."
  task :clear => [:merb_env, :environment] do
    Delayed::Job.delete_all
  end

  desc "Start a delayed_job worker."
  task :work => [:merb_env, :environment] do
    Delayed::Worker.new(:min_priority => ENV['MIN_PRIORITY'], :max_priority => ENV['MAX_PRIORITY']).start
  end
  
  desc "List all pending jobs."
  task :list => [:merb_env, :environment] do
    jobs = Delayed::Job.all
    header = "#{jobs.size} JOBS"
    puts "="*(80-header.size)+header
    puts "ID\tPrty\tAtmps\tHandler\t\tRun at\t\t\tFailed\t\t\tLast Error"
    puts "="*80
    for job in jobs
      puts "#{job.id}\t#{job.priority}\t#{job.attempts}\t#{job.handler.scan(/struct:(.*?)\s+data/).first.first}\t#{job.run_at}\t\t#{job.last_error.first rescue ""}\t"
    end
  end
  
  desc "Start a specific delayed_job."
  task :run => [:merb_env, :environment] do
    Delayed::Job.work_on(ENV['id'])
  end
  
  desc "Unlock a specific delayed_job."
  task :unlock => [:merb_env, :environment] do
    d = Delayed::Job.find(ENV['id'])
    d.unlock
    d.save!
  end
end
