with
tb_distinct_players as
(
    select 
        distinct idPlayer
    from tb_lobby_stats_player
    where idPlayer is not null and idLobbyGame is not null
),

random_players as 
(
    select 
        idPlayer,
        random() as randomPlayerRank
    from tb_distinct_players
    order by randomPlayerRank desc
    limit {nPlayers}
),

player_lobbies as 
(
    select 
        t2.*
    from random_players t1
    left join tb_lobby_stats_player t2
       on t1.idPlayer = t2.idPlayer
),

random_player_lobby as
(
    select 
        *,
        row_number() over (
            partition by idPlayer
            order by random()
        ) as playerLobbyRank
    from player_lobbies
)

select 
	row_number() over (order by idLobbyGame) + (select max(idLobbyGame) from tb_lobby_stats_player) as idLobbyGame,
	idPlayer,
	idRoom,
	qtKill,
	qtAssist,
	qtDeath,
	qtHs,
	qtBombeDefuse,
	qtBombePlant,
	qtTk,
	qtTkAssist,
	qt1Kill,
	qt2Kill,
	qt3Kill,
	qt4Kill,
	qt5Kill,
	qtPlusKill,
	qtFirstKill,
	vlDamage,
	qtHits,
	qtShots,
	qtLastAlive,
	qtClutchWon,
	qtRoundsPlayed,
	descMapName,
	vlLevel,
	qtSurvived,
	qtTrade,
	qtFlashAssist,
	qtHitHeadshot,
	qtHitChest,
	qtHitStomach,
	qtHitLeftAtm,
	qtHitRightArm,
	qtHitLeftLeg,
	qtHitRightLeg,
	flWinner,
	datetime('now') as dtCreatedAt
 from random_player_lobby where playerLobbyRank = 1

 





