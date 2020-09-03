
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

# A subclass of ClusterTask to run spark.
class CbrainTask::SparkHandler < ClusterTask

  Revision_info=CbrainFileRevision[__FILE__] #:nodoc:

  include RestartableTask # This task is naturally restartable
  include RecoverableTask # This task is naturally recoverable

  def self.override_save_results #:nodoc:
    true
  end

  # # See the CbrainTask Programmer Guide
  def save_results #:nodoc:
    params    = self.params

    # Make sure SPARKstage[*]of3 completed successfully by checking its exit status
    # in +exit_cluster_filename+.
    if !File.exists?(exit_cluster_filename)
      self.addlog("Missing exit status file #{exit_cluster_filename}")
      return false
    end

    status_file_content = File.read(exit_cluster_filename).strip
    if status_file_content.blank? || status_file_content !~ /\A^\d+\z/
      self.addlog("Exit status file #{exit_cluster_filename} has unexpected content")
      return false
    else # Check exit status value
      exit_status = status_file_content.to_i
      unless SystemExit.new(exit_status).success?
        self.addlog("Command failed, exit status #{exit_status}")
        return false
      end # content is success
    end # content exists

    if params[:_cb_stage] == "1" || params[:_cb_stage] == "2"
      self.addlog("No need to save results for stage #{params[:_cb_stage]} of Spark")
      return true
    end

    fmri_data = Userfile.find(params[:fmri])

    # DP for destination files
    dest_dp_id = self.results_data_provider_id.presence ||
                 fmri_data.data_provider_id

    output_dir = params[:out_dir]
    self.addlog("Attempting to save results '#{output_dir}'")
    
    cb_out = safe_userfile_find_or_new(FileCollection,
      { :name => output_dir, :data_provider_id => dest_dp_id }
    )

    cb_out.cache_copy_from_local_file(output_dir)
    cb_out.move_to_child_of(fmri_data)

    # Add provenance logs
    self.addlog_to_userfiles_these_created_these( fmri_data, cb_out )

    # Record output file using the Boutiques integrator convention:
    # any params that starts with '_cbrain_output_'.
    self.params['_cbrain_output_result'] = cb_out.id
    self.save

    return true
  end

end

