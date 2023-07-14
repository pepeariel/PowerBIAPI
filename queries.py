query_pedidos_abertos = """

select
case when ftp.site = 'MTLG-PR003' then 'CD Curitiba'
when ftp.site = 'MTLG-PR002' then 'CD Curitiba'
when ftp.site = 'MTLG-PR001' then 'São José Dos Pinhais'
when ftp.site ='MTLG-SC003' then 'CD Itajaí 3'
when ftp.site = 'MTLG-SC002' then 'CD Ag2 Itajaí'
when ftp.site = 'MTLG-SC001' then 'CD Tecnopark Itajaí'
when ftp.site ='MTLG-SP001' then 'CD Barueri'
when ftp.site ='MTLG-SP002' then 'CD Guarulhos'
else ftp.site
end site,
ftp.cliente,
initcap(ftp.nome) as nome,
ftp.pedido,
ftp.cesv,
ftp.nf,
ftp.status,
ftp.qtd_sku,
ftp.qtd_prev,
ftp.qtd_sep,
ftp.qtd_conf,
ftp.qtd_volume,
ftp.qtd_endereco,
ftp.data_integracao,
ftp.inicioseparacao,
ftp.fimseparacao,
ftp.inicioconferencia,
ftp.fimconferencia,
ftp.dt_embarque,
ftp.data_expedicao,
initcap(ftp.destinatario) as destinatario,
ftp.cnpj_dest,
initcap(ftp.transportador) as transportador,
ftp.cnpj_transportador,
initcap(ftp.separador) as separador,
initcap(ftp.conferente) as conferente,
ftp.ingestao_utc_saoPaulo,
case when (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() )) ) <= 4 then '04 hrs'
when (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) > 4 and (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() )) ) <= 8 then '08 hrs'
when (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) > 8 and (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() )) ) <= 12 then '12 hrs'
when (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) > 12 and (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) <= 16 then '16 hrs'
when (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) > 16 and (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) <= 20 then '20 hrs'
when (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) > 20 and (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) <= 24 then '24 hrs'
when (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) > 24 and (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) <= 28 then '28 hrs'
when (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) > 28 and (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) <= 32 then '32 hrs'
when (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) > 32 and (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) <= 36 then '36 hrs'
when (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) > 36 and (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) <= 40 then '40 hrs'
when (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) > 40 and (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) <= 44 then '44 hrs'
when (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) > 44 and (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) <= 48 then '48 hrs'
else 'Mais de 48 hrs'
end faixa_tempo,
case when (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) <= 4 then 4
when (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) > 4 and (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() )) ) <= 8 then 8
when (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) > 8 and (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() )) ) <= 12 then 12
when (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) > 12 and (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) <= 16 then 16
when (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) > 16 and (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) <= 20 then 20
when (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) > 20 and (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) <= 24 then 24
when (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) > 24 and (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) <= 28 then 28
when (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) > 28 and (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) <= 32 then 32
when (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) > 32 and (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) <= 36 then 36
when (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) > 36 and (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) <= 40 then 40
when (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) > 40 and (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) <= 44 then 44
when (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) > 44 and (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() ))) <= 48 then 48
else (datediff(hour, ftp.DATA_INTEGRACAO ,dateadd(hour, -3, getdate() )))
end num_faixa_tempo,
case when ftp.status = 'Novo - Incluído' then '01 Novo' --when ftp.status = 'Novo' and ftp.inicioseparacao is null then '01 Novo'
when ftp.status = 'Alocado' then '02 Alocado' when ftp.status = 'Em separação' then '03 Em separação'
--when ftp.status = 'Novo' and ftp.inicioseparacao is not null then '03 Em Separação'
when ftp.status = 'Em Edição' then '04 Em Edição' when ftp.status = 'Retido' then '05 Retido'
when ftp.status = 'Separado' then '06 Separado'
when ftp.status = 'Separado' and ftp.inicioconferencia is not null and ftp.fimconferencia is null then '07 Em Conferencia'
when ftp.status = 'Conferido' then '08 Conferido'
when ftp.status = 'Carregado' then '09 Carregado'
when ftp.status = 'Expedida' then '10 Expedida'
when ftp.status = 'Cancelado' then '11 Cancelado'
else ftp.status
end situacao
from hive_metastore.ouro_operacao_cd.pedido_aberto ftp


"""