require 'sidekiq'

module Gush
  class Worker
    include Sidekiq::Worker

    sidekiq_retries_exhausted do |msg|
      client = Gush::Client.new
      workflow_id, job = msg['args'][0], client.find_job(*msg['args'])
      job.fail!
      client.persist_job(workflow_id, job)
    end

    def perform(workflow_id, job_id)
      setup_job(workflow_id, job_id)

      job.payloads = incoming_payloads

      error = nil

      mark_as_started
      mark_as_failed and return if job.expired?
      begin
        job.perform
      rescue Job::LoopFail
        client.enqueue_job(workflow_id, job, job.loop_opts[:interval])
      rescue Job::SoftFail
        mark_as_failed(true)
      rescue StandardError
        job.no_retries? ? mark_as_failed : mark_as_enqueued
        raise
      else
        mark_as_finished
        enqueue_outgoing_jobs
        client.restart_workflow(workflow_id, job.params[:clear_job]) if job.params[:clear_job].present?
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

    def mark_as_enqueued
      job.enqueue!
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
