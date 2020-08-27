
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

    # Create stage 1 tasks
    stage1_tasks         = self.create_stage1_tasks

    # Create stage 2 tasks
    grouped_stage2_tasks = self.create_stage2_tasks(stage1_tasks) 

    # Create stage 3 task
    stage3_tasks         = self.create_stage3_tasks(grouped_stage2_tasks)

    return (stage1_tasks + grouped_stage2_tasks + stage3_tasks).flatten
  end

  def create_stage1_tasks #:nodoc:

    # Grab all the cbcsv input files
    cbcsvs       = self.cbcsv_files
    stage1_tasks = []
    # If one or more cbcsv files is present, generate a task for each entry
    # Note they should have been validated in after_form
    if (cbcsvs || []).length > 0
      # Array with the actual userfiles corresponding to the cbcsv
      mapCbcsvToUserfiles = cbcsvs.map { |f| f[1].ordered_raw_ids.map { |i| (i==0) ? nil : i } }
      # Task list to fill and total number of tasks to output
      tasklist, nTasks = [], mapCbcsvToUserfiles[0].length
      # Iterate over each task that needs to be generated
      for i in 0..(nTasks - 1)
        # Clone this task
        currTask = self.dup
        # Replace each cbcsv with an entry
        cbcsvs.map{ |f| f[0] }.each_with_index do |id,j|
          currId = mapCbcsvToUserfiles[j][i]
          #currTask.params[:interface_userfile_ids] << mapCbcsvToUserfiles unless currId.nil?
          currTask.params[id] = currId # If id = 0 or nil, currId = nil
          currTask.params.delete(id) if currId.nil?
        end
        # Add the new task to our tasklist
        currTask.save!
        currTask.params[:out_dir] = currTask.params[:out_dir] + "-#{currTask.id}" + currTask.run_id
        currTask.save!
        stage1_tasks << currTask
      end
    # Default case: just return self as a single task
    else
      self.params[:out_dir] = self.params[:out_dir] + "-#{self.id}" + self.run_id
      self.save!
      stage1_tasks << self
    end
    return stage1_tasks
  end

  def create_stage2_tasks(stage1_tasks) #:nodoc:
    grouped_stage2_tasks = []

    stage2_tool_id = Tool.where(:cbrain_task_class_name => CbrainTask::SPARKstage2of3).first.id
    if !stage2_tool_id
      self.addlog "No tool for Spark stage 2 found."
      return grouped_stage2_tasks
    end

    stage2_tc_id   = ToolConfig.where(:tool_id => stage2_tool_id, 
                                      :bourreau_id => self.bourreau_id).first.id
    if !stage2_tc_id
      self.addlog "Not tool config for Spark stage 2 found."
      return grouped_stage2_tasks
    end

    stage1_tasks.each do |stage1_task|
      nb_resamplings = stage1_task.params[:nb_resamplings]

      stage2_tasks   = []
      
      for job_index in (1..nb_resamplings) do
        stage2_task                = CbrainTask::SPARKstage2of3.new
        stage2_task.user_id        = stage1_task.user_id
        stage2_task.bourreau_id    = stage1_task.bourreau_id
        stage2_task.group_id       = stage1_task.group_id
        stage2_task.tool_config_id = stage2_tc_id
        stage2_task.status         = 'New'

        # Change params.
        stage2_task.params                = params
        stage2_task.params[:jobs_indices] = job_index
        stage2_task.params[:fmri]         = stage1_task.params[:fmri]
        stage2_task.params[:out_dir]      = stage1_task.params[:out_dir]
        stage2_task.params[:verbose]      = stage1_task.params[:verbose]

        stage2_task.share_workdir_with(stage1_task)
        stage2_task.add_prerequisites_for_setup(stage1_task.id, 'Completed')
        
        stage2_task.save!

        stage2_tasks << stage2_task
      end

      grouped_stage2_tasks << stage2_tasks 
    end
    
    return grouped_stage2_tasks
  end

  def create_stage3_tasks(grouped_stage2_tasks) #:nodoc:
    stage3_tasks = []

    stage3_tool_id = Tool.where(:cbrain_task_class_name => CbrainTask::SPARKstage3of3).first.id
    if !stage3_tool_id
      self.addlog "No tool for Spark stage 3 found."
      return stage3_tasks
    end

    stage3_tc_id   = ToolConfig.where(:tool_id => stage3_tool_id, 
                                      :bourreau_id => self.bourreau_id).first.id
    if !stage3_tc_id
      self.addlog "Not tool config for Spark stage 3 found."
      return stage3_tasks
    end

    grouped_stage2_tasks.each do |stage2_tasks| 
      stage2_task                = stage2_tasks.first
      stage3_task                = CbrainTask::SPARKstage3of3.new
      stage3_task.user_id        = stage2_task.user_id
      stage3_task.bourreau_id    = stage2_task.bourreau_id
      stage3_task.group_id       = stage2_task.group_id
      stage3_task.tool_config_id = stage3_tc_id
      stage3_task.status         = 'New'
      
      # Change params.
      stage3_task.params           = params
      stage3_task.params[:fmri]    = stage2_task.params[:fmri]
      stage3_task.params[:out_dir] = stage2_task.params[:out_dir]
      stage3_task.params[:verbose] = stage2_task.params[:verbose]

      stage3_task.share_workdir_with(stage2_task)

      # Add prerequisiteS to stage 2
      stage2_tasks.each do |stage2_task|
        stage3_task.add_prerequisites_for_setup(stage2_task.id, 'Completed')
      end

      stage3_task.save!
      stage3_tasks << stage3_task
    end

    return stage3_tasks
  end

end


