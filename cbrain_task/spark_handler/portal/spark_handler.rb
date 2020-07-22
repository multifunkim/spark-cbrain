
#
# CBRAIN Project
#
# Copyright (C) 2008-2012
# The Royal Institution for the Advancement of Learning
# McGill University
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

# A subclass of PortalTask to launch spark.
class CbrainTask::SparkHandler < PortalTask

  Revision_info=CbrainFileRevision[__FILE__] #:nodoc:


  def self.properties #:nodoc:
    {
       :i_save_my_task_in_after_form       => false, # used by validation code for detecting coding errors
       :i_save_my_tasks_in_final_task_list => true,  # used by validation code for detecting coding errors
    }
  end

  def self.override_final_task_list #:nodoc:
    true
  end

  def final_task_list #:nodoc:
    task_list = []

    # Create stage 1 task
    stage1_task  = self.create_stage1_task
    task_list << stage1_task

    # # Create stage 2 tasks
    stage2_tasks = self.create_stage2_tasks(stage1_task) 
    task_list << stage2_tasks

    # Create stage 3 task
    stage3_task  = self.create_stage3_task(stage2_tasks)
    task_list << stage3_task

    return task_list.flatten
  end

  def create_stage1_task #:nodoc:
    stage1 = CbrainTask::Sparkparallelstage1of3.new
    stage1.user_id          = self.user_id
    stage1.bourreau_id      = self.bourreau_id
    stage1.group_id         = self.group_id
    stage1.tool_config_id   = self.tool_config_id
    stage1.status           = 'New'
    stage1.params           = params
    stage1.params[:stage]   = 1
    stage1.params[:out_dir] = params[:out_dir] + self.run_id

    stage1.save!
    return stage1
  end

  def create_stage2_tasks(stage1) #:nodoc:
    stage2_tasks = []

    nb_resamplings = params[:nb_resamplings]
    
    stage2_tool_id = Tool.where(:cbrain_task_class_name => CbrainTask::Sparkparallelstage2of3).first.id
    if !stage2_tool_id
      self.addlog "No tool for Spark stage 2 found."
      return []
    end

    stage2_tc_id   = ToolConfig.where(:tool_id => stage2_tool_id, 
                                      :bourreau_id => self.bourreau_id).first.id
    if !stage2_tc_id
      self.addlog "Not tool config for Spark stage 2 found."
      return []
    end

    for job_patterns in (0..nb_resamplings) do
      stage2 = CbrainTask::Sparkparallelstage2of3.new
      stage2.user_id          = self.user_id
      stage2.bourreau_id      = self.bourreau_id
      stage2.group_id         = self.group_id
      stage2.tool_config_id   = stage2_tc_id
      stage2.status           = 'New'

      # Change params.
      stage2.params                 = params
      stage2.params[:jobs_patterns] = job_patterns
      stage2.params[:stage]         = 2
      stage2.params[:out_dir]       = stage1.params[:out_dir]

      stage2.share_workdir_with(stage1)
      stage2.add_prerequisites_for_setup(stage1.id, 'Completed')
      
      stage2.save!

      stage2_tasks << stage2
    end
    
    return stage2_tasks
  end

  def create_stage3_task(stage2_tasks) #:nodoc:
    stage2 = stage2_tasks.first
    stage3 = CbrainTask::Sparkparallelstage3of3.new

    stage3_tool_id = Tool.where(:cbrain_task_class_name => CbrainTask::Sparkparallelstage3of3).first.id
    if !stage3_tool_id
      self.addlog "No tool for Spark stage 3 found."
      return []
    end

    stage3_tc_id   = ToolConfig.where(:tool_id => stage3_tool_id, 
                                      :bourreau_id => self.bourreau_id).first.id
    if !stage3_tc_id
      self.addlog "Not tool config for Spark stage 3 found."
      return []
    end

    stage3 = CbrainTask::Sparkparallelstage3of3.new
    stage3.user_id          = self.user_id
    stage3.bourreau_id      = self.bourreau_id
    stage3.group_id         = self.group_id
    stage3.tool_config_id   = stage3_tc_id
    stage3.status           = 'New'
    stage3.params           = params
    stage3.params[:stage3]  = 3

    stage3.share_workdir_with(stage2)

    # Add prerequisiteS to stage 2
    stage2_tasks.each do |stage2_task|
      stage3.params[:out_dir] = stage2_task.params[:out_dir]
      stage3.add_prerequisites_for_setup(stage2.id, 'Completed')
    end

    stage3.save!

    return stage3
  end

end


