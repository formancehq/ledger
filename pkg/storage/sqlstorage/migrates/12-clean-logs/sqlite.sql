--statement
update log set data = json_remove(json_remove(data, '$.preCommitVolumes'), '$.postCommitVolumes');

