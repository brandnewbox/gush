require 'spec_helper'
require 'timecop'

describe Gush::Worker do
  subject { described_class.new }

  let!(:workflow)   { TestWorkflow.create }
  let!(:job)        { client.find_job(workflow.id, "Prepare")  }
  let(:config)      { Gush.configuration.to_json  }
  let!(:client)     { Gush::Client.new }

  describe "#perform" do
    context "when job fails (no retries)" do
      it "should mark it as failed" do
        class FailingJob < Gush::Job
          def perform
            invalid.code_to_raise.error
          end
        end

        class FailingWorkflow < Gush::Workflow
          def configure
            run FailingJob
          end
        end

        workflow = FailingWorkflow.create
        expect do
          subject.perform(workflow.id, "FailingJob")
        end.to raise_error(NameError)
        expect(client.find_job(workflow.id, "FailingJob")).to be_failed
        expect(client.find_job(workflow.id, "FailingJob")).to_not be_failed_softly
      end
    end

    context "when job fails (retries not exhausted)" do
      it "should mark it as failed" do
        class FailingJob < Gush::Job
          def self.sidekiq_options
            { 'retry' => 3 }
          end

          def perform
            invalid.code_to_raise.error
          end
        end

        class FailingWorkflow < Gush::Workflow
          def configure
            run FailingJob
          end
        end

        workflow = FailingWorkflow.create
        expect do
          subject.perform(workflow.id, "FailingJob")
        end.to raise_error(NameError)
        expect(client.find_job(workflow.id, "FailingJob")).to_not be_failed
        expect(client.find_job(workflow.id, "FailingJob")).to be_enqueued
      end
    end

    context "when job softly fails" do
      it "should mark it as failed softly" do
        class FailingJob < Gush::Job
          def perform
            raise Gush::Job::SoftFail
          end
        end

        class FailingWorkflow < Gush::Workflow
          def configure
            run FailingJob
          end
        end

        workflow = FailingWorkflow.create
        expect do
          subject.perform(workflow.id, "FailingJob")
        end.to_not raise_error
        expect(client.find_job(workflow.id, "FailingJob")).to be_failed
        expect(client.find_job(workflow.id, "FailingJob")).to be_failed_softly
      end
    end

    context "when job completes successfully" do
      it "should mark it as succedeed" do
        expect(subject).to receive(:mark_as_finished)

        subject.perform(workflow.id, "Prepare")
      end
    end

    context "when LoopFail is thrown" do
      before do
        class FailingJob < Gush::Job
          def perform
            raise Gush::Job::LoopFail
          end
        end

        class FailingWorkflow < Gush::Workflow
          def configure
            run FailingJob, params: { loop_opts: { interval: 10, end_time: (Time.now + 100).to_i } }
          end
        end
      end

      context "when end_time is not exceeded" do
        it "enqueues the job" do
          workflow = FailingWorkflow.create
          expect do
            subject.perform(workflow.id, "FailingJob")
          end.to_not raise_error
          expect(client.find_job(workflow.id, "FailingJob")).to_not be_failed
          expect(client.find_job(workflow.id, "FailingJob")).to be_enqueued
          expect(Gush::Worker.jobs.size).to eq(1)
        end
      end

      context "when end_time is exceeded" do
        it "marks the job as failed" do
          workflow = FailingWorkflow.create
          Timecop.freeze(Time.now + 200)
          expect do
            subject.perform(workflow.id, "FailingJob")
          end.to_not raise_error
          expect(client.find_job(workflow.id, "FailingJob")).to be_failed
          expect(Gush::Worker.jobs.size).to eq(0)
        end
      end
    end

    it "calls job.perform method" do
      SPY = double()
      expect(SPY).to receive(:some_method)

      class OkayJob < Gush::Job
        def perform
          SPY.some_method
        end
      end

      class OkayWorkflow < Gush::Workflow
        def configure
          run OkayJob
        end
      end

      workflow = OkayWorkflow.create

      subject.perform(workflow.id, 'OkayJob')
    end
  end
end
