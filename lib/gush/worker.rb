require 'sidekiq'

module Gush
  class Worker
    include Sidekiq::Worker

    def perform(workflow_id, job_id)
      setup_job(workflow_id, job_id)

      job.payloads = incoming_payloads

      error = nil

      mark_as_started
      begin
        job.perform
      rescue Job::SoftFail
        mark_as_failed(true)
      rescue StandardError
        mark_as_failed
        raise
      else
        mark_as_finished
        enqueue_outgoing_jobs
      end
    end

    private

    attr_reader :client, :workflow_id, :job

    def client
      @client ||= Gush::Client.new(Gush.configuration)
    end

    def setup_job(workflow_id, job_id)
      @workflow_id = workflow_id
      @job ||= client.find_job(workflow_id, job_id)
    end

    def incoming_payloads
      job.incoming.map do |job_name|
        job = client.find_job(workflow_id, job_name)
        {
          id: job.name,
          class: job.klass.to_s,
          output: job.output_payload
        }
      end
    end

    def mark_as_finished
      job.finish!
      client.persist_job(workflow_id, job)
    end

    def mark_as_failed(soft_fail=false)
      job.fail!(soft_fail)
      client.persist_job(workflow_id, job)
    end

    def mark_as_started
      job.start!
      client.persist_job(workflow_id, job)
    end

    def elapsed(start)
      (Time.now - start).to_f.round(3)
    end

    def enqueue_outgoing_jobs
      job.outgoing.each do |job_name|
        out = client.find_job(workflow_id, job_name)
        if out.ready_to_start?
          client.enqueue_job(workflow_id, out)
        end
      end
    end
  end
end
