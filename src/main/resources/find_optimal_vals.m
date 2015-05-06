options = optimset('MaxFunEvals',1000);
fnames = dir('word_*.m');
numfiles = length(fnames);
optimal_params = zeros(numfiles,2);
optimal_vals = zeros(numfiles);
outFileID = fopen('local_word_params.txt','w');
for i = 1 : numfiles
    [optimal_params(i,:), optimal_vals(i)] = fminsearch(strrep(fnames(i).name, '.m', ''), [10, 10], options);
    fprintf(outFileID, '%s,%f,%f\n', strrep(fnames(i).name, '.m', ''), optimal_params(i,1), optimal_params(i,2));
end
fclose(outFileID);