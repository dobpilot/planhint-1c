/*
 * Copyright (c) 2020 Oleg Kharin, 2023 Alexander Korolev
 *
 */

/* разработан на основании pg_hint_plan.c 
 * из модуля pg_hint_plan https://osdn.net/projects/pghintplan/
 */

#include <string.h>
#include <postgres.h>

#include <fmgr.h>
#include <optimizer/planner.h>
#include <parser/analyze.h>
#include <tcop/utility.h>
#include <commands/prepare.h>
#include <utils/guc.h>

PG_MODULE_MAGIC;

static planner_hook_type	prev_planner_hook = NULL;

static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;
static ProcessUtility_hook_type prev_ProcessUtility_hook = NULL;

/* включен ли режим хинтов */
static bool	planhint1c_enabled = true;

#define GUC_COUNT 2
static const char* guc_names[] = {"enable_nestloop","enable_mergejoin"};
static const char* guc_values[] = {"off","on"};

/* сколько шестнадцатиричных цифр будет в данных */
#define GUCS_DATA_LENGTH (((GUC_COUNT-1)>>1)+1)

#define GUCS_BEGIN_LENGTH 9
static const char* gucs_begin = "'&gH2^7l_";

/* текущий набор устанавливаемых параметров guc */
#define NULL_GUCS 0
#define EMPTY_GUCS 3
static unsigned int cur_gucs = NULL_GUCS; /* битовая маска устанавливаемых переменных, ни одного не установлено по умолчанию, по два бита на переменную */

#define NO_GUCS(gucs) (gucs<=EMPTY_GUCS)

static bool current_gucs_retrieved = false;

/* наш хук для planner_hook */
static PlannedStmt *planhint1c_planner_hook(Query *parse,const char * query_string, int cursorOptions, ParamListInfo boundParams);
/* второй хук для получения текста запроса */
static void planhint1c_post_parse_analyze_hook(ParseState *pstate, Query *query, JumbleState *jstate);
/* хук для сброса разобранных из запроса GUC */
static void planhint1c_plan_ProcessUtility(PlannedStmt *pstmt,
					const char *queryString, bool readOnlyTree,
					ProcessUtilityContext context,
					ParamListInfo params, QueryEnvironment *queryEnv,
					DestReceiver *dest, QueryCompletion *qc);

void _PG_init(void);
void _PG_fini(void);

/* вызывается при подключении модуля в postgresql */
void
_PG_init(void)
{

        DefineCustomBoolVariable("planhint1c.enable",
                         "Enable to use plan hints specified in query texts.",
                                                         NULL,
                                                         &planhint1c_enabled,
                                                         true,
                                                     PGC_USERSET,
                                                         0,
                                                         NULL,
                                                         NULL,
                                                         NULL);

	/* зацепляемся за planner_hook */
	if (planner_hook != planhint1c_planner_hook)
	{
		prev_planner_hook = planner_hook;
		planner_hook = planhint1c_planner_hook;
	}

	/* зацепляемся для получения текста запроса */
	if (post_parse_analyze_hook != planhint1c_post_parse_analyze_hook)
	{
		prev_post_parse_analyze_hook = post_parse_analyze_hook;
		post_parse_analyze_hook = planhint1c_post_parse_analyze_hook;
	}

	/* зацепляемся для сброса разобранных GUC в текущем запросе */
	if (ProcessUtility_hook != planhint1c_plan_ProcessUtility)
	{
		prev_ProcessUtility_hook = ProcessUtility_hook;
		ProcessUtility_hook = planhint1c_plan_ProcessUtility;
	}
}

/*
 * Вызывается при выгрузке модуля из postgresql, в настоящий момент - никогда
 */
void
_PG_fini(void)
{
	/* Uninstall hooks. */
	planner_hook = prev_planner_hook;
	post_parse_analyze_hook = prev_post_parse_analyze_hook;
	ProcessUtility_hook = prev_ProcessUtility_hook;
}


/* функция взята из pg_hint_plan
 * Sets GUC prameters without throwing exception. Reutrns false if something
 * wrong.
 */
static int
set_config_option_noerror(const char *name, const char *value,
						  GucContext context, GucSource source,
						  GucAction action, bool changeVal, int elevel)
{
	int				result = 0;
	MemoryContext	ccxt = CurrentMemoryContext;

	PG_TRY();
	{
		result = set_config_option(name, value, context, source,
								   action, changeVal, 0, false);
	}
	PG_CATCH();
	{
		ErrorData	   *errdata;

		/* Save error info */
		MemoryContextSwitchTo(ccxt);
		errdata = CopyErrorData();
		FlushErrorState();

		ereport(elevel,
				(errcode(errdata->sqlerrcode),
				 errmsg("%s", errdata->message),
				 errdata->detail ? errdetail("%s", errdata->detail) : 0,
				 errdata->hint ? errhint("%s", errdata->hint) : 0));
		FreeErrorData(errdata);
	}
	PG_END_TRY();

	return result;
}

/* устанавливает опции для планировщика согласно полученных из текста запроса */
static void
setup_guc_enforcement(unsigned int gucs,GucContext context)
{
	int	i;
	GucAction action;

	/* получаем вначале область действия */
	switch (gucs & 3)
	{
		case 1: action = GUC_ACTION_SAVE; break; /* только для текущего запроса */
		case 2: action = GUC_ACTION_LOCAL; break; /* до окончания транзакции */
		case 3: action = GUC_ACTION_SET; break;	/* для данного сеанса */
		default: return; /* ошибочное значение */
	}
	gucs >>= 2; /* сдвигаемся на маску */
	/* устанавливаем теперь параметры согласно бит маски */
	for (i = 0; i < GUC_COUNT; i++, gucs >>= 2)
	{
		if (gucs & 2) /* если надо поменять значение глобальной переменной */
		{
			/* пока не нужен нам результат, можно сделать сообщения об ошибках потом
			int			result;
		
			result = */
			set_config_option_noerror(guc_names[i], guc_values[gucs&1], context,
										   PGC_S_SESSION, action, true,
										   INFO);
		}
	}

}

/*
 * Получает строку запроса, переданную клиентом
 */
static const char *
get_query_string(ParseState *pstate, Query *query)
{
	const char *p = debug_query_string;

	/* комментарий из pg_hint_plan
	 * If debug_query_string is set, it is the top level statement. But in some
	 * cases we reach here with debug_query_string set NULL for example in the
	 * case of DESCRIBE message handling or EXECUTE command. We may still see a
	 * candidate top-level query in pstate in the case.
	 */
	if (!p && pstate)
		p = pstate->p_sourcetext;

	/* Не нашли строку запроса, возвращаем NULL */
	if (!p)
		return NULL;

	/* Проверим что запрос имеет тип SELECT или INSERT */
	if (query->commandType == CMD_SELECT || query->commandType == CMD_INSERT)
		return p;
	/* Иначе это может быть запрос EXPLAIN ... */
	else if (query->commandType == CMD_UTILITY)
	{
		Query *target_query = (Query *)query->utilityStmt;

		if (IsA(target_query, ExplainStmt))
		{
			ExplainStmt *stmt = (ExplainStmt *)target_query;
			
			Assert(IsA(stmt->query, Query));
			target_query = (Query *)stmt->query;

			/* strip out the top-level query for further processing */
			if (target_query->commandType == CMD_UTILITY &&
				target_query->utilityStmt != NULL)
				target_query = (Query *)target_query->utilityStmt;

			/* если EXPLAIN требуется для select или insert тогда только будем исследовать строку */
			if (target_query->commandType == CMD_SELECT || target_query->commandType == CMD_INSERT) 
				return p;
		}

	}

	return NULL;
}

/*
 * Вынимает параметры GUC из запроса и возвращает в битовой маске
 */
static unsigned int
get_gucs_from_query(const char *query_str)
{
	const char* e;
	const char* s;
	unsigned int gucs; /* результата присвоения переменных */
	unsigned int pins; /* сюда будем собирать саму маску */

	/* поищем в тексте запроса магическую строку хинтов */
	s = strstr(query_str,gucs_begin);
	if (!s) return EMPTY_GUCS; /* нет магической строки в теле запроса */
	
	s += GUCS_BEGIN_LENGTH; /* переходим на конец магической строки */

	/* будет далее буква области действия: q - запрос, s - сессия, t - до конца транзакции 
 	 *   будем кодировать в младших 2 битах область действия, если там 0 - то неправильно
	 */
	switch (*s)
	{
		case 'q': gucs = 1; break;
		case 't': gucs = 2; break;
		case 's': gucs = 3; break;
		default: return EMPTY_GUCS; /* неправильный вид области действия переменных */
	}
	s++; /* сдвигаемся дальше */
		
	/* разбираем теперь хинты
	 * будет далее буква области действия: q - запрос, s - сессия, t - до конца транзакции  
 	 * пусть они задаются в виде шестнадцатиричного целого числа
 	 */

	/* ищем закрывающую кавычку */
 	e = strchr(s,'\'');
	if (!e) /* ошибка - нет конца */
		return EMPTY_GUCS;
	if (e - s > GUCS_DATA_LENGTH) /* ошибка слишком длинная последовательность символов */
		return EMPTY_GUCS;
	/* зачитаем теперь шестнадцатиричную строку битовой маски установки переменных GUC */
	pins = 0;
	for (; s < e; s++)
	{
		pins <<= 4;
		if (*s < '0' || *s > 'F') return EMPTY_GUCS; /* ошибка - неправильный шестнадцатиричный символ */
		if (*s <= '9') pins += (*s - '0');
		else if (*s >= 'A') pins += (*s - ('A'-10));
		else return EMPTY_GUCS; /* ошибка - неправильный шестнадцатиричный символ */
	}
	
	return gucs+(pins<<2);
		
}


/*
 * Находим опции планировщика из данного запроса.
 */
static void
get_current_gucs(ParseState *pstate, Query *query)
{
	const char *query_str;

	/* если мы уже разбирали ранее, не будем повторно это делать */
	if (current_gucs_retrieved)
		return;

	/* отмечаем, что разбирали уже запрос */
	current_gucs_retrieved = true;

	if (!planhint1c_enabled)
	{
		/* Надо сбросить старые хинты */
		cur_gucs = NULL_GUCS;
		return;
	}

	query_str = get_query_string(pstate, query);

	if (query_str)
	{
		/*
		 * ищем в строке запроса какие хинты для этого запроса будут (что-то надо будет установить)
		 */
		cur_gucs = get_gucs_from_query(query_str);
	}
}

/*
 * Хук перед работой планировщика
 */
static PlannedStmt *
planhint1c_planner_hook(Query *parse, const char * query_string , int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt	*result;
	int			save_nestlevel;

	/*
	 * Будем использовать опции планировщика из текста, если параметр конфигурации palnhint1c есть enabled
	 */	
	if (!planhint1c_enabled) goto standard_planner_proc;

	/* комментарий pg_hint_plan
	 * Query execution in extended protocol can be started without the analyze
	 * phase. In the case retrieve hint string here.
	 */
	if (cur_gucs == NULL_GUCS)
		get_current_gucs(NULL, parse);

 	/* если не удалось опредилить хинты или ничего не устанавливается - ничего делать не будем */
	if (cur_gucs == NULL_GUCS || NO_GUCS(cur_gucs))
		goto standard_planner_proc;

	/*  Set scan enforcement here. */
	save_nestlevel = NewGUCNestLevel();

	/* Apply Set hints, then save it as the initial state  */
	setup_guc_enforcement(cur_gucs,PGC_USERSET);

	/*
	 * Use PG_TRY mechanism to recover GUC parameters and current_hint_state to
	 * the state when this planner started when error occurred in planner.
	 */
	PG_TRY();
	{
		if (prev_planner_hook)
			result = (*prev_planner_hook) (parse, query_string, cursorOptions, boundParams);
		else
			result = standard_planner(parse, query_string, cursorOptions, boundParams);
	}
	PG_CATCH();
	{
		/*
		 * Rollback changes of GUC parameters, and pop current hint context
		 * from hint stack to rewind the state.
		 */
		AtEOXact_GUC(true, save_nestlevel);
		PG_RE_THROW();
	}
	PG_END_TRY();

	/*
	 * current_hint_str is useless after planning of the top-level query.
	 */
	if (cur_gucs != NULL_GUCS)
	{
		cur_gucs = NULL_GUCS;
		current_gucs_retrieved = false;
	}

	/*
	 * Rollback changes of GUC parameters, and pop current hint context from
	 * hint stack to rewind the state.
	 */
	AtEOXact_GUC(true, save_nestlevel);

	return result;
		

standard_planner_proc:
        if (prev_planner_hook)
                return (*prev_planner_hook) (parse, query_string, cursorOptions, boundParams);
        else
                return standard_planner(parse, query_string , cursorOptions, boundParams);

} /* planhint1c_planner_hook() */


/*
 * Хук для получениния опций планировщика из строки запроса
 */
static void
planhint1c_post_parse_analyze_hook(ParseState *pstate, Query *query,JumbleState *jstate)
{
	/* вызываем предыдущие хуки */
	if (prev_post_parse_analyze_hook)
		prev_post_parse_analyze_hook(pstate, query,jstate);

	/* always retrieve hint from the top-level query string */
	current_gucs_retrieved = false;

	get_current_gucs(pstate, query);
}

/* комментарий из pg_hint_plan:
 * We need to reset current_hint_retrieved flag always when a command execution
 * is finished. This is true even for a pure utility command that doesn't
 * involve planning phase.
 * PlannedStmt *pstmt,
										  const char *queryString,
										  bool readOnlyTree,
										  ProcessUtilityContext context,
										  ParamListInfo params,
										  QueryEnvironment *queryEnv,
										  DestReceiver *dest, QueryCompletion *qc)
 */
static void
planhint1c_plan_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
                    bool readOnlyTree, ProcessUtilityContext context,
					ParamListInfo params, QueryEnvironment *queryEnv,
					DestReceiver *dest, QueryCompletion *qc)
{
	if (prev_ProcessUtility_hook)
		prev_ProcessUtility_hook(pstmt,  queryString, readOnlyTree, context, params, queryEnv,
								 dest, qc);
	else
		standard_ProcessUtility(pstmt, queryString, readOnlyTree , context, params, queryEnv,
								 dest, qc);

	current_gucs_retrieved = false;
}

