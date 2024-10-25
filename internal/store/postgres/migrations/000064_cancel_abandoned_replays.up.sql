-- cancel replay requests that are not in terminal state and have not been handled in the last 4 hours
update
    replay_request
set
    status = 'cancelled',
    message = 'canceled by optimus migration 000064'
where
    status in ('created', 'in progress')
    and (now() - updated_at) > INTERVAL '4 HOURS';