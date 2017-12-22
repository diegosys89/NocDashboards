//Version en produccion del Script del Noc QlikSense Application

let FechaAhora = timestamp(now()-1,'YYYY-MM-DD hh:mm:ss');
//set FechaAhora = '$(Fecha)';
//set FechaAhora = '{ts '2017-09-20 00:00:00'}';
//set FechaAhora = {ts '=(timestamp(now()-0.25))'};

LIB CONNECT TO [AR System ODBC Data Source (consulduiohuafc_dsisalima)];

[HPD:Help Desk]:
LOAD
	[Incident Number] AS [Incident Number],
	[Site] AS [Site],
	[Description] AS [Description],
	[Consecuencias] AS [Consecuencias],
	if(isnull([Resolution]) and [Submit Date]>='2016-10-01 00:00:00','Sin Resolution',[Resolution]) AS [Resolution],
	[Categorization Tier 1] AS [Categorization Tier 1],
	[Categorization Tier 2] AS [Categorization Tier 2],
	[Categorization Tier 3] AS [Categorization Tier 3],
	[Product Categorization Tier 1] AS [Product Categorization Tier 1],
	[Product Categorization Tier 2] AS [Product Categorization Tier 2],
	[Product Categorization Tier 3] AS [Product Categorization Tier 3],
	[Resolution Category] AS [Resolution Category],
	[Resolution Category Tier 2] AS [Resolution Category Tier 2],
	[Resolution Category Tier 3] AS [Resolution Category Tier 3],
	[Closure Product Category Tier1] AS [Closure Product Category Tier1],
	[Closure Product Category Tier2] AS [Closure Product Category Tier2],
	[Closure Product Category Tier3] AS [Closure Product Category Tier3],
    [Product Name] as [Product Name],
	[TipoRed] AS [TipoRed],
	[Tipo_Afectacion] AS [Tipo_Afectacion],
	[Urgency] AS [Urgency],
	[Assigned Support Organization] AS [Assigned Support Organization],
	[Assigned Group] AS [Assigned Group],
	[Assignee] AS [Assignee],
	[Start] AS [Start],
	[Finish] AS [Finish],
	[First Name] AS [First Name],
	[Last Name] AS [Last Name],
    [Last Modified By] as [Last Modified By],
    if(isnull([Nombre_Cliente]),'Sin Nombre',[Nombre_Cliente]) as [Operador Apertura],
    if(isnull([Coordenandas]),'Sin Nombre',[Coordenandas]) as [Operador Cierre],
	[Owner Support Organization] AS [Owner Support Organization],
	[Owner Group] AS [Owner Group],
	[Owner] AS [Owner],
	[Status] AS [Status],
    [Submitter] AS [Submitter],
	[Submit Date] AS [Submit Date],
	[Service Type] AS [Service Type],
	[Last Resolved Date] AS [Last Resolved Date],
	[Exclusion] AS [Exclusion],
    [Last Modified Date] AS [Last Modified Date],
    [Status History.Pending.TIME] AS [Status History.Pending.TIME],
	if(isnull([Start]),'Sin Start',if([Submit Date]<[Start],'Start > Submit Date',
    (([Submit Date] - [Start])))) AS [T0],
	if(isnull([Finish]),'Sin Finish',if([Last Resolved Date]<[Finish],'Last Resolved < Finish',
    (([Last Resolved Date] - [Finish])))) AS [T2],
	if(isnull([Finish]),'Sin Finish',if([Start]>[Finish],'Start > Finish',
    if(isnull([Status History.Pending.TIME]) or [Status History.Pending.TIME]>[Finish],([Finish] - [Submit Date]),
    ([Finish] - [Submit Date])-([Finish]-[Status History.Pending.TIME])))) AS [TR],
    [Estado Solucion] AS [Estado Solucion],
    [Notificacion] AS [Notificacion],
    [Jefe_Area] AS [Jefe_Area],
	[Reported to Vendor] AS [Reported to Vendor],
	[Vendor Email] AS [Vendor Email],
    [Vendor First Name] AS [Vendor First Name],
	[Vendor Last Name] AS [Vendor Last Name],
	[Vendor Responded On] AS [Vendor Responded On],
	[Vendor Ticket Number] AS [Vendor Ticket Number],
    [WA] AS [WA]
    ;

SQL SELECT
"Incident Number",
	"Site",
	"Description",
	"Consecuencias",
	"Resolution",
	"Categorization Tier 1",
	"Categorization Tier 2",
	"Categorization Tier 3",
	"Product Categorization Tier 1",
	"Product Categorization Tier 2",
	"Product Categorization Tier 3",
	"Resolution Category",
	"Resolution Category Tier 2",
	"Resolution Category Tier 3",
	"Closure Product Category Tier1",
	"Closure Product Category Tier2",
	"Closure Product Category Tier3",
    "Product Name",
	"TipoRed",
	"Tipo_Afectacion",
	"Urgency",
	"Assigned Support Organization",
	"Assigned Group",
	"Assignee",
	"Start",
	"Finish",
	"First Name",
	"Last Name",
    "Last Modified By",
    "Nombre_Cliente",
    "Coordenandas",
	"Owner Support Organization",
	"Owner Group",
	"Owner",
	"Status",
    "Submitter",
	"Submit Date",
	"Service Type",
	"Last Resolved Date",
	"Exclusion",
    "Last Modified Date",
    "Status History.Pending.TIME",
    "Estado Solucion",
    "Notificacion",
    "Jefe_Area",
    "Reported to Vendor",
    "Vendor Email",
    "Vendor First Name",
    "Vendor Last Name",
    "Vendor Responded On",
    "Vendor Ticket Number",
    "WA"
FROM "HPD:Help Desk" WHERE
("First Name" = 'NOC EXT' OR "First Name" = 'NOC')
and
("Last Name" = 'HUAWEI' OR "Last Name" = 'TELEFONICA ECUADOR')
//and ("Submit Date">={ts '2017-09-20 00:00:00'} or "Last Modified Date">={ts '2017-09-20 00:00:00'});
and ("Submit Date">={ts '$(FechaAhora)'} or "Last Modified Date">={ts '$(FechaAhora)'});

concatenate([HPD:Help Desk])
LOAD * FROM [lib://Data_NOC_HUAWEI (consulduiohuafc_dsisalima)/Incidents_v906.qvd](QVD) where NOT Exists ([Incident Number]);

//COMENTADO POR PROBLEMAS REPLICA

Drop field [WDetail Number v2];

LIB CONNECT TO 'Remedy_Replica (consulduiohuafc_dsisalima)';

left join([HPD:Help Desk])
LOAD `INCIDENT_NUMBER` as [Incident Number],
    Count(`INCIDENT_NUMBER`) AS [WDetail Number v2]
    group by `INCIDENT_NUMBER`;
SQL SELECT `INCIDENT_NUMBER`
FROM PLATAFORMAS.`HPD_HELP_WDETAIL`;

STORE [HPD:Help Desk] INTO [lib://Data_NOC_HUAWEI (consulduiohuafc_dsisalima)/Incidents_v906.qvd];
STORE [HPD:Help Desk] INTO [lib://Data_NOC_HUAWEI (consulduiohuafc_dsisalima)/Incidents_v906.csv](txt);

//Carga de Datos de Relationships -------------------------------

LIB CONNECT TO 'AR System ODBC Data Source (consulduiohuafc_dsisalima)';

Relationship:
LOAD "Request ID02" as [Incident Number],
"Request ID01" as [Related INC],
"Submit Date" as [Submit Date Relationship],
"Request Description01" as [Description Relationship],
"Request ID02" & "Request ID01" as [Relationship ID]
;
SQL SELECT "Request ID01",
"Request ID02", "Submit Date", "Request Description01"
FROM "HPD:Associations" WHERE "Submit Date">={ts '$(FechaAhora)'};

concatenate(Relationship)
LOAD * FROM [lib://Data_NOC_HUAWEI (consulduiohuafc_dsisalima)/Relationship.qvd](QVD) where NOT Exists ([Relationship ID]);

STORE Relationship INTO [lib://Data_NOC_HUAWEI (consulduiohuafc_dsisalima)/Relationship.qvd];
STORE Relationship INTO [lib://Data_NOC_HUAWEI (consulduiohuafc_dsisalima)/Relationship.csv](txt);
//-------------------------------------------------------------------

TicketInfo:
Load [Incident Number],
[Submit Date] as [Reference Date], 'Open' as [Event], if([Status]<>'Cancelled' and
[Resolution]<>'Automated Resolution Reported',1) as [Open Flag],
1 as [Open Flag All],
if(([Assigned Group]='NOC Primer Nivel' or
[Assigned Group]='NOC Segundo Nivel' or
([Assigned Group]='O&M Infraestructura' and [Assignee] = 'TEC - O&M NOC')
or ([Assigned Group]='N1 Redes y Plataformas' and [Assignee]='TEC - O&M N1 REDES Y PLATAFORMAS')
or [Service Type] = 'Infrastructure Restoration') and [First Name] = 'NOC EXT' and [Last Name] = 'HUAWEI' and [Status]<>'Cancelled'
and [Resolution]<>'Automated Resolution Reported',1)
as [Open Flag NOC],

if([Service Type]='Infrastructure Restoration' AND [Product Name]<>'FEMTO INDOOR' and
[First Name] = 'NOC EXT' and
[Last Name] = 'HUAWEI' and
[Status]<>'Cancelled' and
[Resolution]<>'Automated Resolution Reported',
if(floor([T0]*86400)<=1800
//AND floor([TR]*86400)>=900
,'Cumple',
if(([Exclusion]='T0INC' OR [Exclusion]='T0YT2INC'),'Justificado',
//if(floor([TR]*86400)<=900,'Cumple',if(floor([T0]*86400)>1800,'No Cumple'))
'No Cumple'
)),
if([Service Type]='Infrastructure Restoration' AND [Product Name]='FEMTO INDOOR',
if(floor([T0]*86400)<=7200,'Cumple',
if(([Exclusion]='T0INC' OR [Exclusion]='T0YT2INC'),'Justificado',
if(floor([T0]*86400)>7200,'No Cumple'))))) AS [TREa],

if([Service Type]='Infrastructure Restoration' AND [Product Name]<>'FEMTO INDOOR' and
[First Name] = 'NOC EXT' and
[Last Name] = 'HUAWEI' and [Status]<>'Cancelled'
and [Resolution]<>'Automated Resolution Reported',
if(floor([T0]*86400)<=2400 AND floor([TR]*86400)>=900,'Cumple',
if(([Exclusion]='T0INC' OR [Exclusion]='T0YT2INC'),'Justificado',
if(floor([TR]*86400)<=900,'Cumple',if(floor([T0]*86400)>2400,'No Cumple')))),
if([Service Type]='Infrastructure Restoration' AND [Product Name]='FEMTO INDOOR',
'No Aplica')) AS [TNII],

if([Service Type] = 'Infrastructure Restoration' or [Assigned Group] = 'O&M Help Desk'
or [Submitter]='NOCTELE' or [Submitter]='apiusr' or
[First Name] = 'NOC' or
[Last Name] = 'TELEFONICA ECUADOR' or [Status] = 'Cancelled' or
[Resolution] = 'Automated Resolution Reported','No Aplica',
if([Urgency] = '4-Low',if(floor([T0]*86400)<=18000
//and (floor([TR]*86400)>=10800 or [TR] = 'Sin Finish' or [TR] = 'Start > Finish')
,'Cumple',
if(([Exclusion]='T0INC' or [Exclusion]='T0YT2INC' or [Exclusion]='INC'),'Justificado',
//if(([Exclusion]='T0INC' or [Exclusion]='T0YT2INC' or [Exclusion]='INC'),'Justificado','No Cumple')
'No Cumple'
)),
if([Urgency] = '3-Medium',if(floor([T0]*86400)<=7200
//and (floor([TR]*86400)>=5400  or [TR] = 'Sin Finish' or [TR] = 'Start > Finish')
,'Cumple',
if(([Exclusion]='T0INC' or [Exclusion]='T0YT2INC' or [Exclusion]='INC'),'Justificado',
//if(([Exclusion]='T0INC' or [Exclusion]='T0YT2INC' or [Exclusion]='INC'),'Justificado','No Cumple')
'No Cumple'
)),
if([Urgency] = '2-High',if(floor([T0]*86400)<=2700
//and (floor([TR]*86400)>=1800  or [TR] = 'Sin Finish' or [TR] = 'Start > Finish')
,'Cumple',
if(([Exclusion]='T0INC' or [Exclusion]='T0YT2INC' or [Exclusion]='INC'),'Justificado',
//if(([Exclusion]='T0INC' or [Exclusion]='T0YT2INC' or [Exclusion]='INC'),'Justificado','No Cumple')
'No Cumple'
))
,
if([Urgency] = '1-Critical',if(floor([T0]*86400)<=1800
//and (floor([TR]*86400)>=900  or [TR] = 'Sin Finish' or [TR] = 'Start > Finish')
,'Cumple',
if(([Exclusion]='T0INC' or [Exclusion]='T0YT2INC' or [Exclusion]='INC'),'Justificado',
//if(([Exclusion]='T0INC' or [Exclusion]='T0YT2INC' or [Exclusion]='INC'),'Justificado','No Cumple')
'No Cumple'
))
))))
)
as [TREsa]
Resident [HPD:Help Desk];

join(TicketInfo)
Load [Incident Number], [Last Resolved Date] as [Reference Date], 'Closed' as [Event],
if(([Status] = 'Closed' or [Status]='Resolved') and [First Name] = 'NOC EXT' and
[Last Name] = 'HUAWEI' and
[Resolution]<>'Automated Resolution Reported'
and(
[Assigned Group]='NOC Primer Nivel' or
[Assigned Group]='NOC Segundo Nivel' or
([Assigned Group]='O&M Infraestructura' and [Assignee] = 'TEC - O&M NOC')
or ([Assigned Group]='N1 Redes y Plataformas' and [Assignee]='TEC - O&M N1 REDES Y PLATAFORMAS')
or [Service Type] = 'Infrastructure Restoration'),1)
as [Closed Flag],

if(([Status] = 'Closed' or [Status]='Resolved') and [Resolution]<>'Automated Resolution Reported'
,1)  as [Closed Flag All],

if(
if(([Status] = 'Closed' or [Status]='Resolved') and [First Name] = 'NOC EXT' and
[Last Name] = 'HUAWEI' and
[Resolution]<>'Automated Resolution Reported'and (
[Assigned Group]='NOC Primer Nivel' or
[Assigned Group]='NOC Segundo Nivel' or
([Assigned Group]='O&M Infraestructura' and [Assignee] = 'TEC - O&M NOC')
or ([Assigned Group]='N1 Redes y Plataformas' and [Assignee]='TEC - O&M N1 REDES Y PLATAFORMAS')
or [Service Type] = 'Infrastructure Restoration'),1,0) = 1,
if([Service Type]='Infrastructure Restoration'
and [Categorization Tier 1]<>'TEC-SIN AFECTACION DE SERVICIO',IF(floor([TR]*86400)<=14400
or [Estado Solucion] = 'Verificacion', 'Cumple','No Cumple'),
if(floor([TR]*86400)<=28800 or
[Estado Solucion] = 'Verificacion','Cumple',if([Assigned Group]='O&M Help Desk','No Aplica','No Cumple'))),'No Aplica'
) as [TSoE],

if(
if(([Status] = 'Closed' or [Status]='Resolved') and [First Name] = 'NOC EXT' and
[Last Name] = 'HUAWEI' and
[Resolution]<>'Automated Resolution Reported'
and (
[Assigned Group]='NOC Primer Nivel' or
[Assigned Group]='NOC Segundo Nivel' or
([Assigned Group]='O&M Infraestructura' and [Assignee] = 'TEC - O&M NOC')
or ([Assigned Group]='N1 Redes y Plataformas' and [Assignee]='TEC - O&M N1 REDES Y PLATAFORMAS')
or [Service Type] = 'Infrastructure Restoration'),1,0) = 1,
if([Service Type] = 'Infrastructure Restoration'
and [Product Name]<>'FEMTO INDOOR',IF(floor([T2]*86400)<=3300,'Cumple','No Cumple')),'No Aplica'
) as [TNFI]
Resident [HPD:Help Desk];

join(TicketInfo)
Load [Incident Number],
if([Service Type] = 'Infrastructure Restoration','No Aplica',
if(
if(([Status] = 'Closed' or [Status]='Resolved') and [First Name] = 'NOC EXT' and
[Last Name] = 'HUAWEI' and
[Resolution]<>'Automated Resolution Reported'
and(
[Assigned Group]='NOC Primer Nivel' or
[Assigned Group]='NOC Segundo Nivel' or
([Assigned Group]='O&M Infraestructura' and [Assignee] = 'TEC - O&M NOC')),1) = 1,
if(([Urgency] = '1-Critical' and (floor(floor([TR]*86400)/1800)<=[WDetail Number v2]))
or ([Urgency] = '2-High' and (floor(floor([TR]*86400)/7200)<=[WDetail Number v2]))
or ([Urgency] = '3-Medium' and (floor(floor([TR]*86400)/43200)<=[WDetail Number v2]))
or ([Urgency] = '4-Low' and (floor([TR])<=[WDetail Number v2]))
,'Cumple',if([Estado Solucion] = 'Verificacion','Justificado','No Cumple')
)
,'No Aplica'
)) as [FSE]
Resident [HPD:Help Desk];

concatenate(TicketInfo)
Load * from [lib://Data_NOC_HUAWEI (consulduiohuafc_dsisalima)/TicketInfo_v906.qvd](QVD) where NOT Exists ([Incident Number]);

STORE TicketInfo INTO [lib://Data_NOC_HUAWEI (consulduiohuafc_dsisalima)/TicketInfo_v906.qvd];
STORE TicketInfo INTO [lib://Data_NOC_HUAWEI (consulduiohuafc_dsisalima)/TicketInfo_v906.csv](txt);

ListadoPersonal:
LOAD
    NAE as [Operador Cierre],
    Nombre,
    Grupo,
    ACTIVO,
    Calidad,
    "Meta Mensual Calidad",
    Anio,
    Mes
    FROM [lib://Data_NOC_HUAWEI (consulduiohuafc_dsisalima)/ListadoPersonal_DS2.xlsx]
(ooxml, embedded labels, table is Hoja1);

LOAD
    NAE as [Operador Apertura],
    Nombre as [Nombre Apertura]
    FROM [lib://Data_NOC_HUAWEI (consulduiohuafc_dsisalima)/ListadoPersonal_DS2.xlsx]
(ooxml, embedded labels, table is Hoja1);
STORE ListadoPersonal INTO [lib://Data_NOC_HUAWEI (consulduiohuafc_dsisalima)/ListadoPersonal_v906.csv](txt);
