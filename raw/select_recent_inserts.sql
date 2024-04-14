select
    * 
from
    tb_lobby_stats_player
where
    datetime(dtCreatedAt) > {cdc_date}