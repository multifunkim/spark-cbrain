function spark_main(varargin)
% Main function
% Written quickly, input arguments checkings should at least be improved
% Meant to be used by the 'compiled' SPARK version
try
    op = varargin{1};
    if strcmp(op, 'setup')
        setup_spark(varargin{2})
    elseif strcmp(op, 'run')
        run_spark(varargin{2 : end})
    end
catch err
    fprintf(' - An exception occured:\n%s\n', err.message);
    exit(1)
end
end



%% ------------ Local functions ------------------------------------------------
function setup_spark(pipe_opt_file)
% Parsing scheme
valid_fields = {...
    'pipe_file'; ...
    'fmri_data'; 'out_dir'; 'mask'; ...
    'nb_resamplings'; 'network_scales'; 'nb_iterations'; 'p_value'; ...
    'resampling_method'; 'block_window_length'; 'dict_init_method'; ...
    'sparse_coding_method'; 'preserve_dc_atom'; ...
    'verbose'};

p = struct();
[fid, msg] = fopen(pipe_opt_file, 'r');
if fid == -1
    error('\n- Could not open the options file:\n%s\n%s\n', ...
        pipe_opt_file, msg);
end

while true
    s = fgetl(fid);
    if ((numel(s) == 1) && (s == -1))
        break
    end
    
    k = strfind(s, ' ');
    if isempty(k)
        continue
    end
    
    kk = find(strcmp(valid_fields, s(1:k(1)-1)), 1);
    
    if ~isempty(kk)
        p.(valid_fields{kk}) = s(k(1)+1:end);
    end
end
fclose(fid);

if str2double(p.verbose)
    fprintf('\n\n ***** \nUsing the following public parameters:\n')
    disp(p)
    fprintf('\n ***** \n\n')
end


% Private parameters below (for now...)
p.rerun_step1 = '0';
p.rerun_step2 = '0';
p.rerun_step3 = '0';
p.rerun_step4 = '0';
p.sparsity_level = '';
p.network_scale = '';
p.error_flag = '0';
p.display_progress = '1';
p.session_flag = '1';
% p.out_size = 'quality_control'; % useless...
p.test = '1';

% Be safe, some code may forget to append filesep...
p.out_dir = [p.out_dir, filesep];


% Creates the options structure to run SPARK
opt = struct();


% Step 1: Bootstrap resampling
opt.folder_tseries_boot.mask = p.mask;
opt.folder_tseries_boot.nb_samps = str2double(p.nb_resamplings);
opt.folder_tseries_boot.bootstrap.dgp = p.resampling_method;
opt.folder_tseries_boot.bootstrap.block_length = ...
    str_to_reg_spaced_vector(p.block_window_length);
opt.folder_tseries_boot.flag = str2double(p.rerun_step1);


% Step 2: sparse dictionary learning
opt.folder_kmdl = opt.folder_tseries_boot;
opt.folder_kmdl.flag = str2double(p.rerun_step2);
opt.folder_kmdl.ksvd.param = struct(...
    'test_scale', str_to_reg_spaced_vector(p.network_scales), ...
    'numIteration', str2double(p.nb_iterations), ...
    'errorFlag', str2double(p.error_flag), ...
    'preserveDCAtom', str2double(p.preserve_dc_atom), ...
    'InitializationMethod', p.dict_init_method, ...
    'SparsecodingMethod', p.sparse_coding_method, ...
    'displayProgress', str2double(p.display_progress)...
    );
if isempty(p.sparsity_level)
    opt.folder_kmdl.ksvd.param.L = [];
end
if isempty(p.network_scale)
    opt.folder_kmdl.ksvd.param.K = [];
end


% Step 3: spatial clustering
opt.folder_global_dictionary = opt.folder_kmdl;
opt.folder_global_dictionary.flag = str2double(p.rerun_step3);


% Step 4: k-hubness map generation
opt.folder_kmap.nb_samps = opt.folder_tseries_boot.nb_samps;
opt.folder_kmap.ksvd = opt.folder_kmdl.ksvd;
opt.folder_kmap.pvalue = str2double(p.p_value);
opt.folder_kmap.flag = str2double(p.rerun_step4);


% Miscellaneous
opt.flag_session = str2double(p.session_flag);
opt.folder_in = ''; % useless...
opt.folder_out = p.out_dir;
% opt.size_output = p.out_size; % useless...
opt.flag_test = str2double(p.test);


% SPARK pipelines
sep = strfind(p.fmri_data, ' ');
subject_id = p.fmri_data(1 : sep(1)-1);
session_id = p.fmri_data(sep(1)+1 : sep(2)-1);
run_id = p.fmri_data(sep(2)+1 : sep(3)-1);
fmri_file = p.fmri_data(sep(3)+1 : end);
files_in.(subject_id).fmri.(session_id).(run_id) = fmri_file;
[pipe, opt] = spark_pipeline_fmri_kmap(files_in, opt);
pipe = spark_sub_pipelines(pipe);
save(p.pipe_file, 'pipe', 'opt')
end



function x = str_to_reg_spaced_vector(str)
x = cellfun(@(x) str2double(x),strsplit(str,' '));
x = x(1):x(2):x(3);
end



function P = spark_sub_pipelines(pipe)
% Breaks the full SPARK pipeline into sub-pipelines

names = fieldnames(pipe);

ids_stage_A = {'single_kmap'; 'tseries_boot'};
ids_stage_B = {'kmdl_boot'};
ids_stage_C = {'kmdl_Gx'; 'nkmap'};

P.pipe_A = struct();
P.pipe_B = struct();
P.pipe_C = struct();

for k = 1 : size(names, 1)
    name = names{k};
    if startsWith(name, ids_stage_A)
        P.pipe_A.(name) = pipe.(name);
    elseif startsWith(name, ids_stage_B)
        P.pipe_B.(name) = pipe.(name);
    elseif startsWith(name, ids_stage_C)
        P.pipe_C.(name) = pipe.(name);
    end
end
end



function run_spark(varargin)
% Run a SPARK sub-pipeline
pipe_file = varargin{1};
stage = varargin{2};
if nargin > 2
    jobs_patterns = varargin(3 : end);
else
    jobs_patterns = '';
end

if strcmp(stage, 'A')
    pipe = getfield(getfield(load(pipe_file, 'pipe'), 'pipe'), 'pipe_A');
elseif strcmp(stage, 'B')
    pipe = getfield(getfield(load(pipe_file, 'pipe'), 'pipe'), 'pipe_B');
elseif strcmp(stage, 'C')
    pipe = getfield(getfield(load(pipe_file, 'pipe'), 'pipe'), 'pipe_C');
else
    error('Unknown SPARK sub-pipeline: %s', stage)
end

names = fieldnames(pipe);
if endsWith(jobs_patterns, ';')
    names = names(str2num(jobs_patterns)); %#ok
else
    names(~contains(names, jobs_patterns)) = [];
end
for k = 1 : size(names, 1)
    name = names{k};
    files_in = pipe.(name).files_in; %#ok
    files_out = pipe.(name).files_out; %#ok
    opt = pipe.(name).opt;
    private_mkdir(opt.folder_out);
    eval(pipe.(name).command);
end
end



function [status, msg, msgid] = private_mkdir(dir_path)
status = 0; %#ok
msg = ['Failed to create the directory ', dir_path];
msgid = mfilename;

[dir, ~, ext] = fileparts(dir_path);
if ~isempty(ext)
    [status, msg, msgid] = private_mkdir(dir);
elseif (exist(dir, 'dir') || isempty(dir))
    if ~exist(dir_path, 'dir')
        [status, msg, msgid] = mkdir(dir_path);
    else
        status = 1;
    end
else
    [status, msg, msgid] = private_mkdir(dir);
    if status
        if ~exist(dir_path, 'dir')
            [status, msg, msgid] = mkdir(dir_path);
        else
            status = 1;
        end
    end
end
end

