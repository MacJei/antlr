# Generated from TSql.g4 by ANTLR 4.7.2
from antlr4 import *
if __name__ is not None and "." in __name__:
    from .TSqlParser import TSqlParser
else:
    from TSqlParser import TSqlParser

# This class defines a complete generic visitor for a parse tree produced by TSqlParser.

class TSqlVisitor(ParseTreeVisitor):

    # Visit a parse tree produced by TSqlParser#tsql_file.
    def visitTsql_file(self, ctx:TSqlParser.Tsql_fileContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#batch.
    def visitBatch(self, ctx:TSqlParser.BatchContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#sql_clauses.
    def visitSql_clauses(self, ctx:TSqlParser.Sql_clausesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#sql_clause.
    def visitSql_clause(self, ctx:TSqlParser.Sql_clauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#dml_clause.
    def visitDml_clause(self, ctx:TSqlParser.Dml_clauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#cfl_statement.
    def visitCfl_statement(self, ctx:TSqlParser.Cfl_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#block_statement.
    def visitBlock_statement(self, ctx:TSqlParser.Block_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#break_statement.
    def visitBreak_statement(self, ctx:TSqlParser.Break_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#continue_statement.
    def visitContinue_statement(self, ctx:TSqlParser.Continue_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#goto_statement.
    def visitGoto_statement(self, ctx:TSqlParser.Goto_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#return_statement.
    def visitReturn_statement(self, ctx:TSqlParser.Return_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#if_statement.
    def visitIf_statement(self, ctx:TSqlParser.If_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#throw_statement.
    def visitThrow_statement(self, ctx:TSqlParser.Throw_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#throw_error_number.
    def visitThrow_error_number(self, ctx:TSqlParser.Throw_error_numberContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#throw_message.
    def visitThrow_message(self, ctx:TSqlParser.Throw_messageContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#throw_state.
    def visitThrow_state(self, ctx:TSqlParser.Throw_stateContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#try_catch_statement.
    def visitTry_catch_statement(self, ctx:TSqlParser.Try_catch_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#waitfor_statement.
    def visitWaitfor_statement(self, ctx:TSqlParser.Waitfor_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#while_statement.
    def visitWhile_statement(self, ctx:TSqlParser.While_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#print_statement.
    def visitPrint_statement(self, ctx:TSqlParser.Print_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#raiseerror_statement.
    def visitRaiseerror_statement(self, ctx:TSqlParser.Raiseerror_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#empty_statement.
    def visitEmpty_statement(self, ctx:TSqlParser.Empty_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#another_statement.
    def visitAnother_statement(self, ctx:TSqlParser.Another_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#file_path.
    def visitFile_path(self, ctx:TSqlParser.File_pathContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#file_directory_path_separator.
    def visitFile_directory_path_separator(self, ctx:TSqlParser.File_directory_path_separatorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#event_session_predicate_expression.
    def visitEvent_session_predicate_expression(self, ctx:TSqlParser.Event_session_predicate_expressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#event_session_predicate_factor.
    def visitEvent_session_predicate_factor(self, ctx:TSqlParser.Event_session_predicate_factorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#event_session_predicate_leaf.
    def visitEvent_session_predicate_leaf(self, ctx:TSqlParser.Event_session_predicate_leafContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#create_queue.
    def visitCreate_queue(self, ctx:TSqlParser.Create_queueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#queue_settings.
    def visitQueue_settings(self, ctx:TSqlParser.Queue_settingsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#alter_queue.
    def visitAlter_queue(self, ctx:TSqlParser.Alter_queueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#queue_action.
    def visitQueue_action(self, ctx:TSqlParser.Queue_actionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#queue_rebuild_options.
    def visitQueue_rebuild_options(self, ctx:TSqlParser.Queue_rebuild_optionsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#create_contract.
    def visitCreate_contract(self, ctx:TSqlParser.Create_contractContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#conversation_statement.
    def visitConversation_statement(self, ctx:TSqlParser.Conversation_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#message_statement.
    def visitMessage_statement(self, ctx:TSqlParser.Message_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#merge_statement.
    def visitMerge_statement(self, ctx:TSqlParser.Merge_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#merge_matched.
    def visitMerge_matched(self, ctx:TSqlParser.Merge_matchedContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#merge_not_matched.
    def visitMerge_not_matched(self, ctx:TSqlParser.Merge_not_matchedContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#delete_statement.
    def visitDelete_statement(self, ctx:TSqlParser.Delete_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#delete_statement_from.
    def visitDelete_statement_from(self, ctx:TSqlParser.Delete_statement_fromContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#insert_statement.
    def visitInsert_statement(self, ctx:TSqlParser.Insert_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#insert_statement_value.
    def visitInsert_statement_value(self, ctx:TSqlParser.Insert_statement_valueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#receive_statement.
    def visitReceive_statement(self, ctx:TSqlParser.Receive_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#select_statement.
    def visitSelect_statement(self, ctx:TSqlParser.Select_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#time.
    def visitTime(self, ctx:TSqlParser.TimeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#update_statement.
    def visitUpdate_statement(self, ctx:TSqlParser.Update_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#output_clause.
    def visitOutput_clause(self, ctx:TSqlParser.Output_clauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#output_dml_list_elem.
    def visitOutput_dml_list_elem(self, ctx:TSqlParser.Output_dml_list_elemContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#output_column_name.
    def visitOutput_column_name(self, ctx:TSqlParser.Output_column_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#rowset_function_limited.
    def visitRowset_function_limited(self, ctx:TSqlParser.Rowset_function_limitedContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#openquery.
    def visitOpenquery(self, ctx:TSqlParser.OpenqueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#opendatasource.
    def visitOpendatasource(self, ctx:TSqlParser.OpendatasourceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#declare_statement.
    def visitDeclare_statement(self, ctx:TSqlParser.Declare_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#cursor_statement.
    def visitCursor_statement(self, ctx:TSqlParser.Cursor_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#execute_statement.
    def visitExecute_statement(self, ctx:TSqlParser.Execute_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#execute_body.
    def visitExecute_body(self, ctx:TSqlParser.Execute_bodyContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#execute_statement_arg.
    def visitExecute_statement_arg(self, ctx:TSqlParser.Execute_statement_argContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#execute_var_string.
    def visitExecute_var_string(self, ctx:TSqlParser.Execute_var_stringContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#security_statement.
    def visitSecurity_statement(self, ctx:TSqlParser.Security_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#create_certificate.
    def visitCreate_certificate(self, ctx:TSqlParser.Create_certificateContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#existing_keys.
    def visitExisting_keys(self, ctx:TSqlParser.Existing_keysContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#private_key_options.
    def visitPrivate_key_options(self, ctx:TSqlParser.Private_key_optionsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#generate_new_keys.
    def visitGenerate_new_keys(self, ctx:TSqlParser.Generate_new_keysContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#date_options.
    def visitDate_options(self, ctx:TSqlParser.Date_optionsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#open_key.
    def visitOpen_key(self, ctx:TSqlParser.Open_keyContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#close_key.
    def visitClose_key(self, ctx:TSqlParser.Close_keyContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#create_key.
    def visitCreate_key(self, ctx:TSqlParser.Create_keyContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#key_options.
    def visitKey_options(self, ctx:TSqlParser.Key_optionsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#algorithm.
    def visitAlgorithm(self, ctx:TSqlParser.AlgorithmContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#encryption_mechanism.
    def visitEncryption_mechanism(self, ctx:TSqlParser.Encryption_mechanismContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#decryption_mechanism.
    def visitDecryption_mechanism(self, ctx:TSqlParser.Decryption_mechanismContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#grant_permission.
    def visitGrant_permission(self, ctx:TSqlParser.Grant_permissionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#set_statement.
    def visitSet_statement(self, ctx:TSqlParser.Set_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#transaction_statement.
    def visitTransaction_statement(self, ctx:TSqlParser.Transaction_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#go_statement.
    def visitGo_statement(self, ctx:TSqlParser.Go_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#use_statement.
    def visitUse_statement(self, ctx:TSqlParser.Use_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#setuser_statement.
    def visitSetuser_statement(self, ctx:TSqlParser.Setuser_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#execute_clause.
    def visitExecute_clause(self, ctx:TSqlParser.Execute_clauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#declare_local.
    def visitDeclare_local(self, ctx:TSqlParser.Declare_localContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#table_type_definition.
    def visitTable_type_definition(self, ctx:TSqlParser.Table_type_definitionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#xml_type_definition.
    def visitXml_type_definition(self, ctx:TSqlParser.Xml_type_definitionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#xml_schema_collection.
    def visitXml_schema_collection(self, ctx:TSqlParser.Xml_schema_collectionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#column_def_table_constraints.
    def visitColumn_def_table_constraints(self, ctx:TSqlParser.Column_def_table_constraintsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#column_def_table_constraint.
    def visitColumn_def_table_constraint(self, ctx:TSqlParser.Column_def_table_constraintContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#column_definition.
    def visitColumn_definition(self, ctx:TSqlParser.Column_definitionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#materialized_column_definition.
    def visitMaterialized_column_definition(self, ctx:TSqlParser.Materialized_column_definitionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#column_constraint.
    def visitColumn_constraint(self, ctx:TSqlParser.Column_constraintContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#table_constraint.
    def visitTable_constraint(self, ctx:TSqlParser.Table_constraintContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#on_delete.
    def visitOn_delete(self, ctx:TSqlParser.On_deleteContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#on_update.
    def visitOn_update(self, ctx:TSqlParser.On_updateContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#index_options.
    def visitIndex_options(self, ctx:TSqlParser.Index_optionsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#index_option.
    def visitIndex_option(self, ctx:TSqlParser.Index_optionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#declare_cursor.
    def visitDeclare_cursor(self, ctx:TSqlParser.Declare_cursorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#declare_set_cursor_common.
    def visitDeclare_set_cursor_common(self, ctx:TSqlParser.Declare_set_cursor_commonContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#declare_set_cursor_common_partial.
    def visitDeclare_set_cursor_common_partial(self, ctx:TSqlParser.Declare_set_cursor_common_partialContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#fetch_cursor.
    def visitFetch_cursor(self, ctx:TSqlParser.Fetch_cursorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#set_special.
    def visitSet_special(self, ctx:TSqlParser.Set_specialContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#constant_LOCAL_ID.
    def visitConstant_LOCAL_ID(self, ctx:TSqlParser.Constant_LOCAL_IDContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#expression.
    def visitExpression(self, ctx:TSqlParser.ExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#primitive_expression.
    def visitPrimitive_expression(self, ctx:TSqlParser.Primitive_expressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#case_expression.
    def visitCase_expression(self, ctx:TSqlParser.Case_expressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#unary_operator_expression.
    def visitUnary_operator_expression(self, ctx:TSqlParser.Unary_operator_expressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#bracket_expression.
    def visitBracket_expression(self, ctx:TSqlParser.Bracket_expressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#constant_expression.
    def visitConstant_expression(self, ctx:TSqlParser.Constant_expressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#subquery.
    def visitSubquery(self, ctx:TSqlParser.SubqueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#with_expression.
    def visitWith_expression(self, ctx:TSqlParser.With_expressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#common_table_expression.
    def visitCommon_table_expression(self, ctx:TSqlParser.Common_table_expressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#update_elem.
    def visitUpdate_elem(self, ctx:TSqlParser.Update_elemContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#search_condition_list.
    def visitSearch_condition_list(self, ctx:TSqlParser.Search_condition_listContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#search_condition.
    def visitSearch_condition(self, ctx:TSqlParser.Search_conditionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#search_condition_and.
    def visitSearch_condition_and(self, ctx:TSqlParser.Search_condition_andContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#search_condition_not.
    def visitSearch_condition_not(self, ctx:TSqlParser.Search_condition_notContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#predicate.
    def visitPredicate(self, ctx:TSqlParser.PredicateContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#query_expression.
    def visitQuery_expression(self, ctx:TSqlParser.Query_expressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#sql_union.
    def visitSql_union(self, ctx:TSqlParser.Sql_unionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#query_specification.
    def visitQuery_specification(self, ctx:TSqlParser.Query_specificationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#top_clause.
    def visitTop_clause(self, ctx:TSqlParser.Top_clauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#top_percent.
    def visitTop_percent(self, ctx:TSqlParser.Top_percentContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#top_count.
    def visitTop_count(self, ctx:TSqlParser.Top_countContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#order_by_clause.
    def visitOrder_by_clause(self, ctx:TSqlParser.Order_by_clauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#for_clause.
    def visitFor_clause(self, ctx:TSqlParser.For_clauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#xml_common_directives.
    def visitXml_common_directives(self, ctx:TSqlParser.Xml_common_directivesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#order_by_expression.
    def visitOrder_by_expression(self, ctx:TSqlParser.Order_by_expressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#group_by_item.
    def visitGroup_by_item(self, ctx:TSqlParser.Group_by_itemContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#option_clause.
    def visitOption_clause(self, ctx:TSqlParser.Option_clauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#option.
    def visitOption(self, ctx:TSqlParser.OptionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#optimize_for_arg.
    def visitOptimize_for_arg(self, ctx:TSqlParser.Optimize_for_argContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#select_list.
    def visitSelect_list(self, ctx:TSqlParser.Select_listContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#udt_method_arguments.
    def visitUdt_method_arguments(self, ctx:TSqlParser.Udt_method_argumentsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#asterisk.
    def visitAsterisk(self, ctx:TSqlParser.AsteriskContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#column_elem.
    def visitColumn_elem(self, ctx:TSqlParser.Column_elemContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#udt_elem.
    def visitUdt_elem(self, ctx:TSqlParser.Udt_elemContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#expression_elem.
    def visitExpression_elem(self, ctx:TSqlParser.Expression_elemContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#select_list_elem.
    def visitSelect_list_elem(self, ctx:TSqlParser.Select_list_elemContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#table_sources.
    def visitTable_sources(self, ctx:TSqlParser.Table_sourcesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#table_source.
    def visitTable_source(self, ctx:TSqlParser.Table_sourceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#table_source_item_joined.
    def visitTable_source_item_joined(self, ctx:TSqlParser.Table_source_item_joinedContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#table_source_item.
    def visitTable_source_item(self, ctx:TSqlParser.Table_source_itemContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#open_xml.
    def visitOpen_xml(self, ctx:TSqlParser.Open_xmlContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#schema_declaration.
    def visitSchema_declaration(self, ctx:TSqlParser.Schema_declarationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#column_declaration.
    def visitColumn_declaration(self, ctx:TSqlParser.Column_declarationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#change_table.
    def visitChange_table(self, ctx:TSqlParser.Change_tableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#join_part.
    def visitJoin_part(self, ctx:TSqlParser.Join_partContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#pivot_clause.
    def visitPivot_clause(self, ctx:TSqlParser.Pivot_clauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#unpivot_clause.
    def visitUnpivot_clause(self, ctx:TSqlParser.Unpivot_clauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#full_column_name_list.
    def visitFull_column_name_list(self, ctx:TSqlParser.Full_column_name_listContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#table_name_with_hint.
    def visitTable_name_with_hint(self, ctx:TSqlParser.Table_name_with_hintContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#rowset_function.
    def visitRowset_function(self, ctx:TSqlParser.Rowset_functionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#bulk_option.
    def visitBulk_option(self, ctx:TSqlParser.Bulk_optionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#derived_table.
    def visitDerived_table(self, ctx:TSqlParser.Derived_tableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#RANKING_WINDOWED_FUNC.
    def visitRANKING_WINDOWED_FUNC(self, ctx:TSqlParser.RANKING_WINDOWED_FUNCContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#AGGREGATE_WINDOWED_FUNC.
    def visitAGGREGATE_WINDOWED_FUNC(self, ctx:TSqlParser.AGGREGATE_WINDOWED_FUNCContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#ANALYTIC_WINDOWED_FUNC.
    def visitANALYTIC_WINDOWED_FUNC(self, ctx:TSqlParser.ANALYTIC_WINDOWED_FUNCContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#SCALAR_FUNCTION.
    def visitSCALAR_FUNCTION(self, ctx:TSqlParser.SCALAR_FUNCTIONContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#BINARY_CHECKSUM.
    def visitBINARY_CHECKSUM(self, ctx:TSqlParser.BINARY_CHECKSUMContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#CAST.
    def visitCAST(self, ctx:TSqlParser.CASTContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#CONVERT.
    def visitCONVERT(self, ctx:TSqlParser.CONVERTContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#CHECKSUM.
    def visitCHECKSUM(self, ctx:TSqlParser.CHECKSUMContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#COALESCE.
    def visitCOALESCE(self, ctx:TSqlParser.COALESCEContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#CURRENT_TIMESTAMP.
    def visitCURRENT_TIMESTAMP(self, ctx:TSqlParser.CURRENT_TIMESTAMPContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#CURRENT_USER.
    def visitCURRENT_USER(self, ctx:TSqlParser.CURRENT_USERContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#DATEADD.
    def visitDATEADD(self, ctx:TSqlParser.DATEADDContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#DATEDIFF.
    def visitDATEDIFF(self, ctx:TSqlParser.DATEDIFFContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#DATENAME.
    def visitDATENAME(self, ctx:TSqlParser.DATENAMEContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#DATEPART.
    def visitDATEPART(self, ctx:TSqlParser.DATEPARTContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#GETDATE.
    def visitGETDATE(self, ctx:TSqlParser.GETDATEContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#GETUTCDATE.
    def visitGETUTCDATE(self, ctx:TSqlParser.GETUTCDATEContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#IDENTITY.
    def visitIDENTITY(self, ctx:TSqlParser.IDENTITYContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#MIN_ACTIVE_ROWVERSION.
    def visitMIN_ACTIVE_ROWVERSION(self, ctx:TSqlParser.MIN_ACTIVE_ROWVERSIONContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#NULLIF.
    def visitNULLIF(self, ctx:TSqlParser.NULLIFContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#STUFF.
    def visitSTUFF(self, ctx:TSqlParser.STUFFContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#SESSION_USER.
    def visitSESSION_USER(self, ctx:TSqlParser.SESSION_USERContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#SYSTEM_USER.
    def visitSYSTEM_USER(self, ctx:TSqlParser.SYSTEM_USERContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#ISNULL.
    def visitISNULL(self, ctx:TSqlParser.ISNULLContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#XML_DATA_TYPE_FUNC.
    def visitXML_DATA_TYPE_FUNC(self, ctx:TSqlParser.XML_DATA_TYPE_FUNCContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#IFF.
    def visitIFF(self, ctx:TSqlParser.IFFContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#xml_data_type_methods.
    def visitXml_data_type_methods(self, ctx:TSqlParser.Xml_data_type_methodsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#value_method.
    def visitValue_method(self, ctx:TSqlParser.Value_methodContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#query_method.
    def visitQuery_method(self, ctx:TSqlParser.Query_methodContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#exist_method.
    def visitExist_method(self, ctx:TSqlParser.Exist_methodContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#modify_method.
    def visitModify_method(self, ctx:TSqlParser.Modify_methodContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#nodes_method.
    def visitNodes_method(self, ctx:TSqlParser.Nodes_methodContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#switch_section.
    def visitSwitch_section(self, ctx:TSqlParser.Switch_sectionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#switch_search_condition_section.
    def visitSwitch_search_condition_section(self, ctx:TSqlParser.Switch_search_condition_sectionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#as_column_alias.
    def visitAs_column_alias(self, ctx:TSqlParser.As_column_aliasContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#as_table_alias.
    def visitAs_table_alias(self, ctx:TSqlParser.As_table_aliasContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#table_alias.
    def visitTable_alias(self, ctx:TSqlParser.Table_aliasContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#with_table_hints.
    def visitWith_table_hints(self, ctx:TSqlParser.With_table_hintsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#insert_with_table_hints.
    def visitInsert_with_table_hints(self, ctx:TSqlParser.Insert_with_table_hintsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#table_hint.
    def visitTable_hint(self, ctx:TSqlParser.Table_hintContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#index_value.
    def visitIndex_value(self, ctx:TSqlParser.Index_valueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#column_alias_list.
    def visitColumn_alias_list(self, ctx:TSqlParser.Column_alias_listContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#column_alias.
    def visitColumn_alias(self, ctx:TSqlParser.Column_aliasContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#table_value_constructor.
    def visitTable_value_constructor(self, ctx:TSqlParser.Table_value_constructorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#expression_list.
    def visitExpression_list(self, ctx:TSqlParser.Expression_listContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#ranking_windowed_function.
    def visitRanking_windowed_function(self, ctx:TSqlParser.Ranking_windowed_functionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#aggregate_windowed_function.
    def visitAggregate_windowed_function(self, ctx:TSqlParser.Aggregate_windowed_functionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#analytic_windowed_function.
    def visitAnalytic_windowed_function(self, ctx:TSqlParser.Analytic_windowed_functionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#all_distinct_expression.
    def visitAll_distinct_expression(self, ctx:TSqlParser.All_distinct_expressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#over_clause.
    def visitOver_clause(self, ctx:TSqlParser.Over_clauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#row_or_range_clause.
    def visitRow_or_range_clause(self, ctx:TSqlParser.Row_or_range_clauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#window_frame_extent.
    def visitWindow_frame_extent(self, ctx:TSqlParser.Window_frame_extentContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#window_frame_bound.
    def visitWindow_frame_bound(self, ctx:TSqlParser.Window_frame_boundContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#window_frame_preceding.
    def visitWindow_frame_preceding(self, ctx:TSqlParser.Window_frame_precedingContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#window_frame_following.
    def visitWindow_frame_following(self, ctx:TSqlParser.Window_frame_followingContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#full_table_name.
    def visitFull_table_name(self, ctx:TSqlParser.Full_table_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#table_name.
    def visitTable_name(self, ctx:TSqlParser.Table_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#func_proc_name_schema.
    def visitFunc_proc_name_schema(self, ctx:TSqlParser.Func_proc_name_schemaContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#func_proc_name_database_schema.
    def visitFunc_proc_name_database_schema(self, ctx:TSqlParser.Func_proc_name_database_schemaContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#func_proc_name_server_database_schema.
    def visitFunc_proc_name_server_database_schema(self, ctx:TSqlParser.Func_proc_name_server_database_schemaContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#ddl_object.
    def visitDdl_object(self, ctx:TSqlParser.Ddl_objectContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#full_column_name.
    def visitFull_column_name(self, ctx:TSqlParser.Full_column_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#column_name_list_with_order.
    def visitColumn_name_list_with_order(self, ctx:TSqlParser.Column_name_list_with_orderContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#column_name_list.
    def visitColumn_name_list(self, ctx:TSqlParser.Column_name_listContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#cursor_name.
    def visitCursor_name(self, ctx:TSqlParser.Cursor_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#on_off.
    def visitOn_off(self, ctx:TSqlParser.On_offContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#clustered.
    def visitClustered(self, ctx:TSqlParser.ClusteredContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#null_notnull.
    def visitNull_notnull(self, ctx:TSqlParser.Null_notnullContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#null_or_default.
    def visitNull_or_default(self, ctx:TSqlParser.Null_or_defaultContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#scalar_function_name.
    def visitScalar_function_name(self, ctx:TSqlParser.Scalar_function_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#begin_conversation_timer.
    def visitBegin_conversation_timer(self, ctx:TSqlParser.Begin_conversation_timerContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#begin_conversation_dialog.
    def visitBegin_conversation_dialog(self, ctx:TSqlParser.Begin_conversation_dialogContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#contract_name.
    def visitContract_name(self, ctx:TSqlParser.Contract_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#service_name.
    def visitService_name(self, ctx:TSqlParser.Service_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#end_conversation.
    def visitEnd_conversation(self, ctx:TSqlParser.End_conversationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#waitfor_conversation.
    def visitWaitfor_conversation(self, ctx:TSqlParser.Waitfor_conversationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#get_conversation.
    def visitGet_conversation(self, ctx:TSqlParser.Get_conversationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#queue_id.
    def visitQueue_id(self, ctx:TSqlParser.Queue_idContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#send_conversation.
    def visitSend_conversation(self, ctx:TSqlParser.Send_conversationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#data_type.
    def visitData_type(self, ctx:TSqlParser.Data_typeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#constant.
    def visitConstant(self, ctx:TSqlParser.ConstantContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#sign.
    def visitSign(self, ctx:TSqlParser.SignContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#identifier.
    def visitIdentifier(self, ctx:TSqlParser.IdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#simple_id.
    def visitSimple_id(self, ctx:TSqlParser.Simple_idContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#comparison_operator.
    def visitComparison_operator(self, ctx:TSqlParser.Comparison_operatorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TSqlParser#assignment_operator.
    def visitAssignment_operator(self, ctx:TSqlParser.Assignment_operatorContext):
        return self.visitChildren(ctx)



del TSqlParser