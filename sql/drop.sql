SELECT table_name
FROM information_schema.views
WHERE table_schema = 'ETHEREUM'
AND table_owner LIKE 'ABI_VIEW_MANAGER_%';
