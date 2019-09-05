# Generated from TSql.g4 by ANTLR 4.7.2
from antlr4 import *
if __name__ is not None and "." in __name__:
    from .TSqlParser import TSqlParser
else:
    from TSqlParser import TSqlParser

# This class defines a complete listener for a parse tree produced by TSqlParser.
class TSqlListener(ParseTreeListener):

    # Enter a parse tree produced by TSqlParser#tsql_file.
    def enterTsql_file(self, ctx:TSqlParser.Tsql_fileContext):
        pass

    # Exit a parse tree produced by TSqlParser#tsql_file.
    def exitTsql_file(self, ctx:TSqlParser.Tsql_fileContext):
        pass


    # Enter a parse tree produced by TSqlParser#batch.
    def enterBatch(self, ctx:TSqlParser.BatchContext):
        pass

    # Exit a parse tree produced by TSqlParser#batch.
    def exitBatch(self, ctx:TSqlParser.BatchContext):
        pass


    # Enter a parse tree produced by TSqlParser#sql_clauses.
    def enterSql_clauses(self, ctx:TSqlParser.Sql_clausesContext):
        pass

    # Exit a parse tree produced by TSqlParser#sql_clauses.
    def exitSql_clauses(self, ctx:TSqlParser.Sql_clausesContext):
        pass


    # Enter a parse tree produced by TSqlParser#sql_clause.
    def enterSql_clause(self, ctx:TSqlParser.Sql_clauseContext):
        pass

    # Exit a parse tree produced by TSqlParser#sql_clause.
    def exitSql_clause(self, ctx:TSqlParser.Sql_clauseContext):
        pass


    # Enter a parse tree produced by TSqlParser#dml_clause.
    def enterDml_clause(self, ctx:TSqlParser.Dml_clauseContext):
        pass

    # Exit a parse tree produced by TSqlParser#dml_clause.
    def exitDml_clause(self, ctx:TSqlParser.Dml_clauseContext):
        pass


    # Enter a parse tree produced by TSqlParser#cfl_statement.
    def enterCfl_statement(self, ctx:TSqlParser.Cfl_statementContext):
        pass

    # Exit a parse tree produced by TSqlParser#cfl_statement.
    def exitCfl_statement(self, ctx:TSqlParser.Cfl_statementContext):
        pass


    # Enter a parse tree produced by TSqlParser#block_statement.
    def enterBlock_statement(self, ctx:TSqlParser.Block_statementContext):
        pass

    # Exit a parse tree produced by TSqlParser#block_statement.
    def exitBlock_statement(self, ctx:TSqlParser.Block_statementContext):
        pass


    # Enter a parse tree produced by TSqlParser#break_statement.
    def enterBreak_statement(self, ctx:TSqlParser.Break_statementContext):
        pass

    # Exit a parse tree produced by TSqlParser#break_statement.
    def exitBreak_statement(self, ctx:TSqlParser.Break_statementContext):
        pass


    # Enter a parse tree produced by TSqlParser#continue_statement.
    def enterContinue_statement(self, ctx:TSqlParser.Continue_statementContext):
        pass

    # Exit a parse tree produced by TSqlParser#continue_statement.
    def exitContinue_statement(self, ctx:TSqlParser.Continue_statementContext):
        pass


    # Enter a parse tree produced by TSqlParser#goto_statement.
    def enterGoto_statement(self, ctx:TSqlParser.Goto_statementContext):
        pass

    # Exit a parse tree produced by TSqlParser#goto_statement.
    def exitGoto_statement(self, ctx:TSqlParser.Goto_statementContext):
        pass


    # Enter a parse tree produced by TSqlParser#return_statement.
    def enterReturn_statement(self, ctx:TSqlParser.Return_statementContext):
        pass

    # Exit a parse tree produced by TSqlParser#return_statement.
    def exitReturn_statement(self, ctx:TSqlParser.Return_statementContext):
        pass


    # Enter a parse tree produced by TSqlParser#if_statement.
    def enterIf_statement(self, ctx:TSqlParser.If_statementContext):
        pass

    # Exit a parse tree produced by TSqlParser#if_statement.
    def exitIf_statement(self, ctx:TSqlParser.If_statementContext):
        pass


    # Enter a parse tree produced by TSqlParser#throw_statement.
    def enterThrow_statement(self, ctx:TSqlParser.Throw_statementContext):
        pass

    # Exit a parse tree produced by TSqlParser#throw_statement.
    def exitThrow_statement(self, ctx:TSqlParser.Throw_statementContext):
        pass


    # Enter a parse tree produced by TSqlParser#throw_error_number.
    def enterThrow_error_number(self, ctx:TSqlParser.Throw_error_numberContext):
        pass

    # Exit a parse tree produced by TSqlParser#throw_error_number.
    def exitThrow_error_number(self, ctx:TSqlParser.Throw_error_numberContext):
        pass


    # Enter a parse tree produced by TSqlParser#throw_message.
    def enterThrow_message(self, ctx:TSqlParser.Throw_messageContext):
        pass

    # Exit a parse tree produced by TSqlParser#throw_message.
    def exitThrow_message(self, ctx:TSqlParser.Throw_messageContext):
        pass


    # Enter a parse tree produced by TSqlParser#throw_state.
    def enterThrow_state(self, ctx:TSqlParser.Throw_stateContext):
        pass

    # Exit a parse tree produced by TSqlParser#throw_state.
    def exitThrow_state(self, ctx:TSqlParser.Throw_stateContext):
        pass


    # Enter a parse tree produced by TSqlParser#try_catch_statement.
    def enterTry_catch_statement(self, ctx:TSqlParser.Try_catch_statementContext):
        pass

    # Exit a parse tree produced by TSqlParser#try_catch_statement.
    def exitTry_catch_statement(self, ctx:TSqlParser.Try_catch_statementContext):
        pass


    # Enter a parse tree produced by TSqlParser#waitfor_statement.
    def enterWaitfor_statement(self, ctx:TSqlParser.Waitfor_statementContext):
        pass

    # Exit a parse tree produced by TSqlParser#waitfor_statement.
    def exitWaitfor_statement(self, ctx:TSqlParser.Waitfor_statementContext):
        pass


    # Enter a parse tree produced by TSqlParser#while_statement.
    def enterWhile_statement(self, ctx:TSqlParser.While_statementContext):
        pass

    # Exit a parse tree produced by TSqlParser#while_statement.
    def exitWhile_statement(self, ctx:TSqlParser.While_statementContext):
        pass


    # Enter a parse tree produced by TSqlParser#print_statement.
    def enterPrint_statement(self, ctx:TSqlParser.Print_statementContext):
        pass

    # Exit a parse tree produced by TSqlParser#print_statement.
    def exitPrint_statement(self, ctx:TSqlParser.Print_statementContext):
        pass


    # Enter a parse tree produced by TSqlParser#raiseerror_statement.
    def enterRaiseerror_statement(self, ctx:TSqlParser.Raiseerror_statementContext):
        pass

    # Exit a parse tree produced by TSqlParser#raiseerror_statement.
    def exitRaiseerror_statement(self, ctx:TSqlParser.Raiseerror_statementContext):
        pass


    # Enter a parse tree produced by TSqlParser#empty_statement.
    def enterEmpty_statement(self, ctx:TSqlParser.Empty_statementContext):
        pass

    # Exit a parse tree produced by TSqlParser#empty_statement.
    def exitEmpty_statement(self, ctx:TSqlParser.Empty_statementContext):
        pass


    # Enter a parse tree produced by TSqlParser#another_statement.
    def enterAnother_statement(self, ctx:TSqlParser.Another_statementContext):
        pass

    # Exit a parse tree produced by TSqlParser#another_statement.
    def exitAnother_statement(self, ctx:TSqlParser.Another_statementContext):
        pass


    # Enter a parse tree produced by TSqlParser#file_path.
    def enterFile_path(self, ctx:TSqlParser.File_pathContext):
        pass

    # Exit a parse tree produced by TSqlParser#file_path.
    def exitFile_path(self, ctx:TSqlParser.File_pathContext):
        pass


    # Enter a parse tree produced by TSqlParser#file_directory_path_separator.
    def enterFile_directory_path_separator(self, ctx:TSqlParser.File_directory_path_separatorContext):
        pass

    # Exit a parse tree produced by TSqlParser#file_directory_path_separator.
    def exitFile_directory_path_separator(self, ctx:TSqlParser.File_directory_path_separatorContext):
        pass


    # Enter a parse tree produced by TSqlParser#event_session_predicate_expression.
    def enterEvent_session_predicate_expression(self, ctx:TSqlParser.Event_session_predicate_expressionContext):
        pass

    # Exit a parse tree produced by TSqlParser#event_session_predicate_expression.
    def exitEvent_session_predicate_expression(self, ctx:TSqlParser.Event_session_predicate_expressionContext):
        pass


    # Enter a parse tree produced by TSqlParser#event_session_predicate_factor.
    def enterEvent_session_predicate_factor(self, ctx:TSqlParser.Event_session_predicate_factorContext):
        pass

    # Exit a parse tree produced by TSqlParser#event_session_predicate_factor.
    def exitEvent_session_predicate_factor(self, ctx:TSqlParser.Event_session_predicate_factorContext):
        pass


    # Enter a parse tree produced by TSqlParser#event_session_predicate_leaf.
    def enterEvent_session_predicate_leaf(self, ctx:TSqlParser.Event_session_predicate_leafContext):
        pass

    # Exit a parse tree produced by TSqlParser#event_session_predicate_leaf.
    def exitEvent_session_predicate_leaf(self, ctx:TSqlParser.Event_session_predicate_leafContext):
        pass


    # Enter a parse tree produced by TSqlParser#create_queue.
    def enterCreate_queue(self, ctx:TSqlParser.Create_queueContext):
        pass

    # Exit a parse tree produced by TSqlParser#create_queue.
    def exitCreate_queue(self, ctx:TSqlParser.Create_queueContext):
        pass


    # Enter a parse tree produced by TSqlParser#queue_settings.
    def enterQueue_settings(self, ctx:TSqlParser.Queue_settingsContext):
        pass

    # Exit a parse tree produced by TSqlParser#queue_settings.
    def exitQueue_settings(self, ctx:TSqlParser.Queue_settingsContext):
        pass


    # Enter a parse tree produced by TSqlParser#alter_queue.
    def enterAlter_queue(self, ctx:TSqlParser.Alter_queueContext):
        pass

    # Exit a parse tree produced by TSqlParser#alter_queue.
    def exitAlter_queue(self, ctx:TSqlParser.Alter_queueContext):
        pass


    # Enter a parse tree produced by TSqlParser#queue_action.
    def enterQueue_action(self, ctx:TSqlParser.Queue_actionContext):
        pass

    # Exit a parse tree produced by TSqlParser#queue_action.
    def exitQueue_action(self, ctx:TSqlParser.Queue_actionContext):
        pass


    # Enter a parse tree produced by TSqlParser#queue_rebuild_options.
    def enterQueue_rebuild_options(self, ctx:TSqlParser.Queue_rebuild_optionsContext):
        pass

    # Exit a parse tree produced by TSqlParser#queue_rebuild_options.
    def exitQueue_rebuild_options(self, ctx:TSqlParser.Queue_rebuild_optionsContext):
        pass


    # Enter a parse tree produced by TSqlParser#create_contract.
    def enterCreate_contract(self, ctx:TSqlParser.Create_contractContext):
        pass

    # Exit a parse tree produced by TSqlParser#create_contract.
    def exitCreate_contract(self, ctx:TSqlParser.Create_contractContext):
        pass


    # Enter a parse tree produced by TSqlParser#conversation_statement.
    def enterConversation_statement(self, ctx:TSqlParser.Conversation_statementContext):
        pass

    # Exit a parse tree produced by TSqlParser#conversation_statement.
    def exitConversation_statement(self, ctx:TSqlParser.Conversation_statementContext):
        pass


    # Enter a parse tree produced by TSqlParser#message_statement.
    def enterMessage_statement(self, ctx:TSqlParser.Message_statementContext):
        pass

    # Exit a parse tree produced by TSqlParser#message_statement.
    def exitMessage_statement(self, ctx:TSqlParser.Message_statementContext):
        pass


    # Enter a parse tree produced by TSqlParser#merge_statement.
    def enterMerge_statement(self, ctx:TSqlParser.Merge_statementContext):
        pass

    # Exit a parse tree produced by TSqlParser#merge_statement.
    def exitMerge_statement(self, ctx:TSqlParser.Merge_statementContext):
        pass


    # Enter a parse tree produced by TSqlParser#merge_matched.
    def enterMerge_matched(self, ctx:TSqlParser.Merge_matchedContext):
        pass

    # Exit a parse tree produced by TSqlParser#merge_matched.
    def exitMerge_matched(self, ctx:TSqlParser.Merge_matchedContext):
        pass


    # Enter a parse tree produced by TSqlParser#merge_not_matched.
    def enterMerge_not_matched(self, ctx:TSqlParser.Merge_not_matchedContext):
        pass

    # Exit a parse tree produced by TSqlParser#merge_not_matched.
    def exitMerge_not_matched(self, ctx:TSqlParser.Merge_not_matchedContext):
        pass


    # Enter a parse tree produced by TSqlParser#delete_statement.
    def enterDelete_statement(self, ctx:TSqlParser.Delete_statementContext):
        pass

    # Exit a parse tree produced by TSqlParser#delete_statement.
    def exitDelete_statement(self, ctx:TSqlParser.Delete_statementContext):
        pass


    # Enter a parse tree produced by TSqlParser#delete_statement_from.
    def enterDelete_statement_from(self, ctx:TSqlParser.Delete_statement_fromContext):
        pass

    # Exit a parse tree produced by TSqlParser#delete_statement_from.
    def exitDelete_statement_from(self, ctx:TSqlParser.Delete_statement_fromContext):
        pass


    # Enter a parse tree produced by TSqlParser#insert_statement.
    def enterInsert_statement(self, ctx:TSqlParser.Insert_statementContext):
        pass

    # Exit a parse tree produced by TSqlParser#insert_statement.
    def exitInsert_statement(self, ctx:TSqlParser.Insert_statementContext):
        pass


    # Enter a parse tree produced by TSqlParser#insert_statement_value.
    def enterInsert_statement_value(self, ctx:TSqlParser.Insert_statement_valueContext):
        pass

    # Exit a parse tree produced by TSqlParser#insert_statement_value.
    def exitInsert_statement_value(self, ctx:TSqlParser.Insert_statement_valueContext):
        pass


    # Enter a parse tree produced by TSqlParser#receive_statement.
    def enterReceive_statement(self, ctx:TSqlParser.Receive_statementContext):
        pass

    # Exit a parse tree produced by TSqlParser#receive_statement.
    def exitReceive_statement(self, ctx:TSqlParser.Receive_statementContext):
        pass


    # Enter a parse tree produced by TSqlParser#select_statement.
    def enterSelect_statement(self, ctx:TSqlParser.Select_statementContext):
        pass

    # Exit a parse tree produced by TSqlParser#select_statement.
    def exitSelect_statement(self, ctx:TSqlParser.Select_statementContext):
        pass


    # Enter a parse tree produced by TSqlParser#time.
    def enterTime(self, ctx:TSqlParser.TimeContext):
        pass

    # Exit a parse tree produced by TSqlParser#time.
    def exitTime(self, ctx:TSqlParser.TimeContext):
        pass


    # Enter a parse tree produced by TSqlParser#update_statement.
    def enterUpdate_statement(self, ctx:TSqlParser.Update_statementContext):
        pass

    # Exit a parse tree produced by TSqlParser#update_statement.
    def exitUpdate_statement(self, ctx:TSqlParser.Update_statementContext):
        pass


    # Enter a parse tree produced by TSqlParser#output_clause.
    def enterOutput_clause(self, ctx:TSqlParser.Output_clauseContext):
        pass

    # Exit a parse tree produced by TSqlParser#output_clause.
    def exitOutput_clause(self, ctx:TSqlParser.Output_clauseContext):
        pass


    # Enter a parse tree produced by TSqlParser#output_dml_list_elem.
    def enterOutput_dml_list_elem(self, ctx:TSqlParser.Output_dml_list_elemContext):
        pass

    # Exit a parse tree produced by TSqlParser#output_dml_list_elem.
    def exitOutput_dml_list_elem(self, ctx:TSqlParser.Output_dml_list_elemContext):
        pass


    # Enter a parse tree produced by TSqlParser#output_column_name.
    def enterOutput_column_name(self, ctx:TSqlParser.Output_column_nameContext):
        pass

    # Exit a parse tree produced by TSqlParser#output_column_name.
    def exitOutput_column_name(self, ctx:TSqlParser.Output_column_nameContext):
        pass


    # Enter a parse tree produced by TSqlParser#rowset_function_limited.
    def enterRowset_function_limited(self, ctx:TSqlParser.Rowset_function_limitedContext):
        pass

    # Exit a parse tree produced by TSqlParser#rowset_function_limited.
    def exitRowset_function_limited(self, ctx:TSqlParser.Rowset_function_limitedContext):
        pass


    # Enter a parse tree produced by TSqlParser#openquery.
    def enterOpenquery(self, ctx:TSqlParser.OpenqueryContext):
        pass

    # Exit a parse tree produced by TSqlParser#openquery.
    def exitOpenquery(self, ctx:TSqlParser.OpenqueryContext):
        pass


    # Enter a parse tree produced by TSqlParser#opendatasource.
    def enterOpendatasource(self, ctx:TSqlParser.OpendatasourceContext):
        pass

    # Exit a parse tree produced by TSqlParser#opendatasource.
    def exitOpendatasource(self, ctx:TSqlParser.OpendatasourceContext):
        pass


    # Enter a parse tree produced by TSqlParser#declare_statement.
    def enterDeclare_statement(self, ctx:TSqlParser.Declare_statementContext):
        pass

    # Exit a parse tree produced by TSqlParser#declare_statement.
    def exitDeclare_statement(self, ctx:TSqlParser.Declare_statementContext):
        pass


    # Enter a parse tree produced by TSqlParser#cursor_statement.
    def enterCursor_statement(self, ctx:TSqlParser.Cursor_statementContext):
        pass

    # Exit a parse tree produced by TSqlParser#cursor_statement.
    def exitCursor_statement(self, ctx:TSqlParser.Cursor_statementContext):
        pass


    # Enter a parse tree produced by TSqlParser#execute_statement.
    def enterExecute_statement(self, ctx:TSqlParser.Execute_statementContext):
        pass

    # Exit a parse tree produced by TSqlParser#execute_statement.
    def exitExecute_statement(self, ctx:TSqlParser.Execute_statementContext):
        pass


    # Enter a parse tree produced by TSqlParser#execute_body.
    def enterExecute_body(self, ctx:TSqlParser.Execute_bodyContext):
        pass

    # Exit a parse tree produced by TSqlParser#execute_body.
    def exitExecute_body(self, ctx:TSqlParser.Execute_bodyContext):
        pass


    # Enter a parse tree produced by TSqlParser#execute_statement_arg.
    def enterExecute_statement_arg(self, ctx:TSqlParser.Execute_statement_argContext):
        pass

    # Exit a parse tree produced by TSqlParser#execute_statement_arg.
    def exitExecute_statement_arg(self, ctx:TSqlParser.Execute_statement_argContext):
        pass


    # Enter a parse tree produced by TSqlParser#execute_var_string.
    def enterExecute_var_string(self, ctx:TSqlParser.Execute_var_stringContext):
        pass

    # Exit a parse tree produced by TSqlParser#execute_var_string.
    def exitExecute_var_string(self, ctx:TSqlParser.Execute_var_stringContext):
        pass


    # Enter a parse tree produced by TSqlParser#security_statement.
    def enterSecurity_statement(self, ctx:TSqlParser.Security_statementContext):
        pass

    # Exit a parse tree produced by TSqlParser#security_statement.
    def exitSecurity_statement(self, ctx:TSqlParser.Security_statementContext):
        pass


    # Enter a parse tree produced by TSqlParser#create_certificate.
    def enterCreate_certificate(self, ctx:TSqlParser.Create_certificateContext):
        pass

    # Exit a parse tree produced by TSqlParser#create_certificate.
    def exitCreate_certificate(self, ctx:TSqlParser.Create_certificateContext):
        pass


    # Enter a parse tree produced by TSqlParser#existing_keys.
    def enterExisting_keys(self, ctx:TSqlParser.Existing_keysContext):
        pass

    # Exit a parse tree produced by TSqlParser#existing_keys.
    def exitExisting_keys(self, ctx:TSqlParser.Existing_keysContext):
        pass


    # Enter a parse tree produced by TSqlParser#private_key_options.
    def enterPrivate_key_options(self, ctx:TSqlParser.Private_key_optionsContext):
        pass

    # Exit a parse tree produced by TSqlParser#private_key_options.
    def exitPrivate_key_options(self, ctx:TSqlParser.Private_key_optionsContext):
        pass


    # Enter a parse tree produced by TSqlParser#generate_new_keys.
    def enterGenerate_new_keys(self, ctx:TSqlParser.Generate_new_keysContext):
        pass

    # Exit a parse tree produced by TSqlParser#generate_new_keys.
    def exitGenerate_new_keys(self, ctx:TSqlParser.Generate_new_keysContext):
        pass


    # Enter a parse tree produced by TSqlParser#date_options.
    def enterDate_options(self, ctx:TSqlParser.Date_optionsContext):
        pass

    # Exit a parse tree produced by TSqlParser#date_options.
    def exitDate_options(self, ctx:TSqlParser.Date_optionsContext):
        pass


    # Enter a parse tree produced by TSqlParser#open_key.
    def enterOpen_key(self, ctx:TSqlParser.Open_keyContext):
        pass

    # Exit a parse tree produced by TSqlParser#open_key.
    def exitOpen_key(self, ctx:TSqlParser.Open_keyContext):
        pass


    # Enter a parse tree produced by TSqlParser#close_key.
    def enterClose_key(self, ctx:TSqlParser.Close_keyContext):
        pass

    # Exit a parse tree produced by TSqlParser#close_key.
    def exitClose_key(self, ctx:TSqlParser.Close_keyContext):
        pass


    # Enter a parse tree produced by TSqlParser#create_key.
    def enterCreate_key(self, ctx:TSqlParser.Create_keyContext):
        pass

    # Exit a parse tree produced by TSqlParser#create_key.
    def exitCreate_key(self, ctx:TSqlParser.Create_keyContext):
        pass


    # Enter a parse tree produced by TSqlParser#key_options.
    def enterKey_options(self, ctx:TSqlParser.Key_optionsContext):
        pass

    # Exit a parse tree produced by TSqlParser#key_options.
    def exitKey_options(self, ctx:TSqlParser.Key_optionsContext):
        pass


    # Enter a parse tree produced by TSqlParser#algorithm.
    def enterAlgorithm(self, ctx:TSqlParser.AlgorithmContext):
        pass

    # Exit a parse tree produced by TSqlParser#algorithm.
    def exitAlgorithm(self, ctx:TSqlParser.AlgorithmContext):
        pass


    # Enter a parse tree produced by TSqlParser#encryption_mechanism.
    def enterEncryption_mechanism(self, ctx:TSqlParser.Encryption_mechanismContext):
        pass

    # Exit a parse tree produced by TSqlParser#encryption_mechanism.
    def exitEncryption_mechanism(self, ctx:TSqlParser.Encryption_mechanismContext):
        pass


    # Enter a parse tree produced by TSqlParser#decryption_mechanism.
    def enterDecryption_mechanism(self, ctx:TSqlParser.Decryption_mechanismContext):
        pass

    # Exit a parse tree produced by TSqlParser#decryption_mechanism.
    def exitDecryption_mechanism(self, ctx:TSqlParser.Decryption_mechanismContext):
        pass


    # Enter a parse tree produced by TSqlParser#grant_permission.
    def enterGrant_permission(self, ctx:TSqlParser.Grant_permissionContext):
        pass

    # Exit a parse tree produced by TSqlParser#grant_permission.
    def exitGrant_permission(self, ctx:TSqlParser.Grant_permissionContext):
        pass


    # Enter a parse tree produced by TSqlParser#set_statement.
    def enterSet_statement(self, ctx:TSqlParser.Set_statementContext):
        pass

    # Exit a parse tree produced by TSqlParser#set_statement.
    def exitSet_statement(self, ctx:TSqlParser.Set_statementContext):
        pass


    # Enter a parse tree produced by TSqlParser#transaction_statement.
    def enterTransaction_statement(self, ctx:TSqlParser.Transaction_statementContext):
        pass

    # Exit a parse tree produced by TSqlParser#transaction_statement.
    def exitTransaction_statement(self, ctx:TSqlParser.Transaction_statementContext):
        pass


    # Enter a parse tree produced by TSqlParser#go_statement.
    def enterGo_statement(self, ctx:TSqlParser.Go_statementContext):
        pass

    # Exit a parse tree produced by TSqlParser#go_statement.
    def exitGo_statement(self, ctx:TSqlParser.Go_statementContext):
        pass


    # Enter a parse tree produced by TSqlParser#use_statement.
    def enterUse_statement(self, ctx:TSqlParser.Use_statementContext):
        pass

    # Exit a parse tree produced by TSqlParser#use_statement.
    def exitUse_statement(self, ctx:TSqlParser.Use_statementContext):
        pass


    # Enter a parse tree produced by TSqlParser#setuser_statement.
    def enterSetuser_statement(self, ctx:TSqlParser.Setuser_statementContext):
        pass

    # Exit a parse tree produced by TSqlParser#setuser_statement.
    def exitSetuser_statement(self, ctx:TSqlParser.Setuser_statementContext):
        pass


    # Enter a parse tree produced by TSqlParser#execute_clause.
    def enterExecute_clause(self, ctx:TSqlParser.Execute_clauseContext):
        pass

    # Exit a parse tree produced by TSqlParser#execute_clause.
    def exitExecute_clause(self, ctx:TSqlParser.Execute_clauseContext):
        pass


    # Enter a parse tree produced by TSqlParser#declare_local.
    def enterDeclare_local(self, ctx:TSqlParser.Declare_localContext):
        pass

    # Exit a parse tree produced by TSqlParser#declare_local.
    def exitDeclare_local(self, ctx:TSqlParser.Declare_localContext):
        pass


    # Enter a parse tree produced by TSqlParser#table_type_definition.
    def enterTable_type_definition(self, ctx:TSqlParser.Table_type_definitionContext):
        pass

    # Exit a parse tree produced by TSqlParser#table_type_definition.
    def exitTable_type_definition(self, ctx:TSqlParser.Table_type_definitionContext):
        pass


    # Enter a parse tree produced by TSqlParser#xml_type_definition.
    def enterXml_type_definition(self, ctx:TSqlParser.Xml_type_definitionContext):
        pass

    # Exit a parse tree produced by TSqlParser#xml_type_definition.
    def exitXml_type_definition(self, ctx:TSqlParser.Xml_type_definitionContext):
        pass


    # Enter a parse tree produced by TSqlParser#xml_schema_collection.
    def enterXml_schema_collection(self, ctx:TSqlParser.Xml_schema_collectionContext):
        pass

    # Exit a parse tree produced by TSqlParser#xml_schema_collection.
    def exitXml_schema_collection(self, ctx:TSqlParser.Xml_schema_collectionContext):
        pass


    # Enter a parse tree produced by TSqlParser#column_def_table_constraints.
    def enterColumn_def_table_constraints(self, ctx:TSqlParser.Column_def_table_constraintsContext):
        pass

    # Exit a parse tree produced by TSqlParser#column_def_table_constraints.
    def exitColumn_def_table_constraints(self, ctx:TSqlParser.Column_def_table_constraintsContext):
        pass


    # Enter a parse tree produced by TSqlParser#column_def_table_constraint.
    def enterColumn_def_table_constraint(self, ctx:TSqlParser.Column_def_table_constraintContext):
        pass

    # Exit a parse tree produced by TSqlParser#column_def_table_constraint.
    def exitColumn_def_table_constraint(self, ctx:TSqlParser.Column_def_table_constraintContext):
        pass


    # Enter a parse tree produced by TSqlParser#column_definition.
    def enterColumn_definition(self, ctx:TSqlParser.Column_definitionContext):
        pass

    # Exit a parse tree produced by TSqlParser#column_definition.
    def exitColumn_definition(self, ctx:TSqlParser.Column_definitionContext):
        pass


    # Enter a parse tree produced by TSqlParser#materialized_column_definition.
    def enterMaterialized_column_definition(self, ctx:TSqlParser.Materialized_column_definitionContext):
        pass

    # Exit a parse tree produced by TSqlParser#materialized_column_definition.
    def exitMaterialized_column_definition(self, ctx:TSqlParser.Materialized_column_definitionContext):
        pass


    # Enter a parse tree produced by TSqlParser#column_constraint.
    def enterColumn_constraint(self, ctx:TSqlParser.Column_constraintContext):
        pass

    # Exit a parse tree produced by TSqlParser#column_constraint.
    def exitColumn_constraint(self, ctx:TSqlParser.Column_constraintContext):
        pass


    # Enter a parse tree produced by TSqlParser#table_constraint.
    def enterTable_constraint(self, ctx:TSqlParser.Table_constraintContext):
        pass

    # Exit a parse tree produced by TSqlParser#table_constraint.
    def exitTable_constraint(self, ctx:TSqlParser.Table_constraintContext):
        pass


    # Enter a parse tree produced by TSqlParser#on_delete.
    def enterOn_delete(self, ctx:TSqlParser.On_deleteContext):
        pass

    # Exit a parse tree produced by TSqlParser#on_delete.
    def exitOn_delete(self, ctx:TSqlParser.On_deleteContext):
        pass


    # Enter a parse tree produced by TSqlParser#on_update.
    def enterOn_update(self, ctx:TSqlParser.On_updateContext):
        pass

    # Exit a parse tree produced by TSqlParser#on_update.
    def exitOn_update(self, ctx:TSqlParser.On_updateContext):
        pass


    # Enter a parse tree produced by TSqlParser#index_options.
    def enterIndex_options(self, ctx:TSqlParser.Index_optionsContext):
        pass

    # Exit a parse tree produced by TSqlParser#index_options.
    def exitIndex_options(self, ctx:TSqlParser.Index_optionsContext):
        pass


    # Enter a parse tree produced by TSqlParser#index_option.
    def enterIndex_option(self, ctx:TSqlParser.Index_optionContext):
        pass

    # Exit a parse tree produced by TSqlParser#index_option.
    def exitIndex_option(self, ctx:TSqlParser.Index_optionContext):
        pass


    # Enter a parse tree produced by TSqlParser#declare_cursor.
    def enterDeclare_cursor(self, ctx:TSqlParser.Declare_cursorContext):
        pass

    # Exit a parse tree produced by TSqlParser#declare_cursor.
    def exitDeclare_cursor(self, ctx:TSqlParser.Declare_cursorContext):
        pass


    # Enter a parse tree produced by TSqlParser#declare_set_cursor_common.
    def enterDeclare_set_cursor_common(self, ctx:TSqlParser.Declare_set_cursor_commonContext):
        pass

    # Exit a parse tree produced by TSqlParser#declare_set_cursor_common.
    def exitDeclare_set_cursor_common(self, ctx:TSqlParser.Declare_set_cursor_commonContext):
        pass


    # Enter a parse tree produced by TSqlParser#declare_set_cursor_common_partial.
    def enterDeclare_set_cursor_common_partial(self, ctx:TSqlParser.Declare_set_cursor_common_partialContext):
        pass

    # Exit a parse tree produced by TSqlParser#declare_set_cursor_common_partial.
    def exitDeclare_set_cursor_common_partial(self, ctx:TSqlParser.Declare_set_cursor_common_partialContext):
        pass


    # Enter a parse tree produced by TSqlParser#fetch_cursor.
    def enterFetch_cursor(self, ctx:TSqlParser.Fetch_cursorContext):
        pass

    # Exit a parse tree produced by TSqlParser#fetch_cursor.
    def exitFetch_cursor(self, ctx:TSqlParser.Fetch_cursorContext):
        pass


    # Enter a parse tree produced by TSqlParser#set_special.
    def enterSet_special(self, ctx:TSqlParser.Set_specialContext):
        pass

    # Exit a parse tree produced by TSqlParser#set_special.
    def exitSet_special(self, ctx:TSqlParser.Set_specialContext):
        pass


    # Enter a parse tree produced by TSqlParser#constant_LOCAL_ID.
    def enterConstant_LOCAL_ID(self, ctx:TSqlParser.Constant_LOCAL_IDContext):
        pass

    # Exit a parse tree produced by TSqlParser#constant_LOCAL_ID.
    def exitConstant_LOCAL_ID(self, ctx:TSqlParser.Constant_LOCAL_IDContext):
        pass


    # Enter a parse tree produced by TSqlParser#expression.
    def enterExpression(self, ctx:TSqlParser.ExpressionContext):
        pass

    # Exit a parse tree produced by TSqlParser#expression.
    def exitExpression(self, ctx:TSqlParser.ExpressionContext):
        pass


    # Enter a parse tree produced by TSqlParser#primitive_expression.
    def enterPrimitive_expression(self, ctx:TSqlParser.Primitive_expressionContext):
        pass

    # Exit a parse tree produced by TSqlParser#primitive_expression.
    def exitPrimitive_expression(self, ctx:TSqlParser.Primitive_expressionContext):
        pass


    # Enter a parse tree produced by TSqlParser#case_expression.
    def enterCase_expression(self, ctx:TSqlParser.Case_expressionContext):
        pass

    # Exit a parse tree produced by TSqlParser#case_expression.
    def exitCase_expression(self, ctx:TSqlParser.Case_expressionContext):
        pass


    # Enter a parse tree produced by TSqlParser#unary_operator_expression.
    def enterUnary_operator_expression(self, ctx:TSqlParser.Unary_operator_expressionContext):
        pass

    # Exit a parse tree produced by TSqlParser#unary_operator_expression.
    def exitUnary_operator_expression(self, ctx:TSqlParser.Unary_operator_expressionContext):
        pass


    # Enter a parse tree produced by TSqlParser#bracket_expression.
    def enterBracket_expression(self, ctx:TSqlParser.Bracket_expressionContext):
        pass

    # Exit a parse tree produced by TSqlParser#bracket_expression.
    def exitBracket_expression(self, ctx:TSqlParser.Bracket_expressionContext):
        pass


    # Enter a parse tree produced by TSqlParser#constant_expression.
    def enterConstant_expression(self, ctx:TSqlParser.Constant_expressionContext):
        pass

    # Exit a parse tree produced by TSqlParser#constant_expression.
    def exitConstant_expression(self, ctx:TSqlParser.Constant_expressionContext):
        pass


    # Enter a parse tree produced by TSqlParser#subquery.
    def enterSubquery(self, ctx:TSqlParser.SubqueryContext):
        pass

    # Exit a parse tree produced by TSqlParser#subquery.
    def exitSubquery(self, ctx:TSqlParser.SubqueryContext):
        pass


    # Enter a parse tree produced by TSqlParser#with_expression.
    def enterWith_expression(self, ctx:TSqlParser.With_expressionContext):
        pass

    # Exit a parse tree produced by TSqlParser#with_expression.
    def exitWith_expression(self, ctx:TSqlParser.With_expressionContext):
        pass


    # Enter a parse tree produced by TSqlParser#common_table_expression.
    def enterCommon_table_expression(self, ctx:TSqlParser.Common_table_expressionContext):
        pass

    # Exit a parse tree produced by TSqlParser#common_table_expression.
    def exitCommon_table_expression(self, ctx:TSqlParser.Common_table_expressionContext):
        pass


    # Enter a parse tree produced by TSqlParser#update_elem.
    def enterUpdate_elem(self, ctx:TSqlParser.Update_elemContext):
        pass

    # Exit a parse tree produced by TSqlParser#update_elem.
    def exitUpdate_elem(self, ctx:TSqlParser.Update_elemContext):
        pass


    # Enter a parse tree produced by TSqlParser#search_condition_list.
    def enterSearch_condition_list(self, ctx:TSqlParser.Search_condition_listContext):
        pass

    # Exit a parse tree produced by TSqlParser#search_condition_list.
    def exitSearch_condition_list(self, ctx:TSqlParser.Search_condition_listContext):
        pass


    # Enter a parse tree produced by TSqlParser#search_condition.
    def enterSearch_condition(self, ctx:TSqlParser.Search_conditionContext):
        pass

    # Exit a parse tree produced by TSqlParser#search_condition.
    def exitSearch_condition(self, ctx:TSqlParser.Search_conditionContext):
        pass


    # Enter a parse tree produced by TSqlParser#search_condition_and.
    def enterSearch_condition_and(self, ctx:TSqlParser.Search_condition_andContext):
        pass

    # Exit a parse tree produced by TSqlParser#search_condition_and.
    def exitSearch_condition_and(self, ctx:TSqlParser.Search_condition_andContext):
        pass


    # Enter a parse tree produced by TSqlParser#search_condition_not.
    def enterSearch_condition_not(self, ctx:TSqlParser.Search_condition_notContext):
        pass

    # Exit a parse tree produced by TSqlParser#search_condition_not.
    def exitSearch_condition_not(self, ctx:TSqlParser.Search_condition_notContext):
        pass


    # Enter a parse tree produced by TSqlParser#predicate.
    def enterPredicate(self, ctx:TSqlParser.PredicateContext):
        pass

    # Exit a parse tree produced by TSqlParser#predicate.
    def exitPredicate(self, ctx:TSqlParser.PredicateContext):
        pass


    # Enter a parse tree produced by TSqlParser#query_expression.
    def enterQuery_expression(self, ctx:TSqlParser.Query_expressionContext):
        pass

    # Exit a parse tree produced by TSqlParser#query_expression.
    def exitQuery_expression(self, ctx:TSqlParser.Query_expressionContext):
        pass


    # Enter a parse tree produced by TSqlParser#sql_union.
    def enterSql_union(self, ctx:TSqlParser.Sql_unionContext):
        pass

    # Exit a parse tree produced by TSqlParser#sql_union.
    def exitSql_union(self, ctx:TSqlParser.Sql_unionContext):
        pass


    # Enter a parse tree produced by TSqlParser#query_specification.
    def enterQuery_specification(self, ctx:TSqlParser.Query_specificationContext):
        pass

    # Exit a parse tree produced by TSqlParser#query_specification.
    def exitQuery_specification(self, ctx:TSqlParser.Query_specificationContext):
        pass


    # Enter a parse tree produced by TSqlParser#top_clause.
    def enterTop_clause(self, ctx:TSqlParser.Top_clauseContext):
        pass

    # Exit a parse tree produced by TSqlParser#top_clause.
    def exitTop_clause(self, ctx:TSqlParser.Top_clauseContext):
        pass


    # Enter a parse tree produced by TSqlParser#top_percent.
    def enterTop_percent(self, ctx:TSqlParser.Top_percentContext):
        pass

    # Exit a parse tree produced by TSqlParser#top_percent.
    def exitTop_percent(self, ctx:TSqlParser.Top_percentContext):
        pass


    # Enter a parse tree produced by TSqlParser#top_count.
    def enterTop_count(self, ctx:TSqlParser.Top_countContext):
        pass

    # Exit a parse tree produced by TSqlParser#top_count.
    def exitTop_count(self, ctx:TSqlParser.Top_countContext):
        pass


    # Enter a parse tree produced by TSqlParser#order_by_clause.
    def enterOrder_by_clause(self, ctx:TSqlParser.Order_by_clauseContext):
        pass

    # Exit a parse tree produced by TSqlParser#order_by_clause.
    def exitOrder_by_clause(self, ctx:TSqlParser.Order_by_clauseContext):
        pass


    # Enter a parse tree produced by TSqlParser#for_clause.
    def enterFor_clause(self, ctx:TSqlParser.For_clauseContext):
        pass

    # Exit a parse tree produced by TSqlParser#for_clause.
    def exitFor_clause(self, ctx:TSqlParser.For_clauseContext):
        pass


    # Enter a parse tree produced by TSqlParser#xml_common_directives.
    def enterXml_common_directives(self, ctx:TSqlParser.Xml_common_directivesContext):
        pass

    # Exit a parse tree produced by TSqlParser#xml_common_directives.
    def exitXml_common_directives(self, ctx:TSqlParser.Xml_common_directivesContext):
        pass


    # Enter a parse tree produced by TSqlParser#order_by_expression.
    def enterOrder_by_expression(self, ctx:TSqlParser.Order_by_expressionContext):
        pass

    # Exit a parse tree produced by TSqlParser#order_by_expression.
    def exitOrder_by_expression(self, ctx:TSqlParser.Order_by_expressionContext):
        pass


    # Enter a parse tree produced by TSqlParser#group_by_item.
    def enterGroup_by_item(self, ctx:TSqlParser.Group_by_itemContext):
        pass

    # Exit a parse tree produced by TSqlParser#group_by_item.
    def exitGroup_by_item(self, ctx:TSqlParser.Group_by_itemContext):
        pass


    # Enter a parse tree produced by TSqlParser#option_clause.
    def enterOption_clause(self, ctx:TSqlParser.Option_clauseContext):
        pass

    # Exit a parse tree produced by TSqlParser#option_clause.
    def exitOption_clause(self, ctx:TSqlParser.Option_clauseContext):
        pass


    # Enter a parse tree produced by TSqlParser#option.
    def enterOption(self, ctx:TSqlParser.OptionContext):
        pass

    # Exit a parse tree produced by TSqlParser#option.
    def exitOption(self, ctx:TSqlParser.OptionContext):
        pass


    # Enter a parse tree produced by TSqlParser#optimize_for_arg.
    def enterOptimize_for_arg(self, ctx:TSqlParser.Optimize_for_argContext):
        pass

    # Exit a parse tree produced by TSqlParser#optimize_for_arg.
    def exitOptimize_for_arg(self, ctx:TSqlParser.Optimize_for_argContext):
        pass


    # Enter a parse tree produced by TSqlParser#select_list.
    def enterSelect_list(self, ctx:TSqlParser.Select_listContext):
        pass

    # Exit a parse tree produced by TSqlParser#select_list.
    def exitSelect_list(self, ctx:TSqlParser.Select_listContext):
        pass


    # Enter a parse tree produced by TSqlParser#udt_method_arguments.
    def enterUdt_method_arguments(self, ctx:TSqlParser.Udt_method_argumentsContext):
        pass

    # Exit a parse tree produced by TSqlParser#udt_method_arguments.
    def exitUdt_method_arguments(self, ctx:TSqlParser.Udt_method_argumentsContext):
        pass


    # Enter a parse tree produced by TSqlParser#asterisk.
    def enterAsterisk(self, ctx:TSqlParser.AsteriskContext):
        pass

    # Exit a parse tree produced by TSqlParser#asterisk.
    def exitAsterisk(self, ctx:TSqlParser.AsteriskContext):
        pass


    # Enter a parse tree produced by TSqlParser#column_elem.
    def enterColumn_elem(self, ctx:TSqlParser.Column_elemContext):
        pass

    # Exit a parse tree produced by TSqlParser#column_elem.
    def exitColumn_elem(self, ctx:TSqlParser.Column_elemContext):
        pass


    # Enter a parse tree produced by TSqlParser#udt_elem.
    def enterUdt_elem(self, ctx:TSqlParser.Udt_elemContext):
        pass

    # Exit a parse tree produced by TSqlParser#udt_elem.
    def exitUdt_elem(self, ctx:TSqlParser.Udt_elemContext):
        pass


    # Enter a parse tree produced by TSqlParser#expression_elem.
    def enterExpression_elem(self, ctx:TSqlParser.Expression_elemContext):
        pass

    # Exit a parse tree produced by TSqlParser#expression_elem.
    def exitExpression_elem(self, ctx:TSqlParser.Expression_elemContext):
        pass


    # Enter a parse tree produced by TSqlParser#select_list_elem.
    def enterSelect_list_elem(self, ctx:TSqlParser.Select_list_elemContext):
        pass

    # Exit a parse tree produced by TSqlParser#select_list_elem.
    def exitSelect_list_elem(self, ctx:TSqlParser.Select_list_elemContext):
        pass


    # Enter a parse tree produced by TSqlParser#table_sources.
    def enterTable_sources(self, ctx:TSqlParser.Table_sourcesContext):
        pass

    # Exit a parse tree produced by TSqlParser#table_sources.
    def exitTable_sources(self, ctx:TSqlParser.Table_sourcesContext):
        pass


    # Enter a parse tree produced by TSqlParser#table_source.
    def enterTable_source(self, ctx:TSqlParser.Table_sourceContext):
        pass

    # Exit a parse tree produced by TSqlParser#table_source.
    def exitTable_source(self, ctx:TSqlParser.Table_sourceContext):
        pass


    # Enter a parse tree produced by TSqlParser#table_source_item_joined.
    def enterTable_source_item_joined(self, ctx:TSqlParser.Table_source_item_joinedContext):
        pass

    # Exit a parse tree produced by TSqlParser#table_source_item_joined.
    def exitTable_source_item_joined(self, ctx:TSqlParser.Table_source_item_joinedContext):
        pass


    # Enter a parse tree produced by TSqlParser#table_source_item.
    def enterTable_source_item(self, ctx:TSqlParser.Table_source_itemContext):
        pass

    # Exit a parse tree produced by TSqlParser#table_source_item.
    def exitTable_source_item(self, ctx:TSqlParser.Table_source_itemContext):
        pass


    # Enter a parse tree produced by TSqlParser#open_xml.
    def enterOpen_xml(self, ctx:TSqlParser.Open_xmlContext):
        pass

    # Exit a parse tree produced by TSqlParser#open_xml.
    def exitOpen_xml(self, ctx:TSqlParser.Open_xmlContext):
        pass


    # Enter a parse tree produced by TSqlParser#schema_declaration.
    def enterSchema_declaration(self, ctx:TSqlParser.Schema_declarationContext):
        pass

    # Exit a parse tree produced by TSqlParser#schema_declaration.
    def exitSchema_declaration(self, ctx:TSqlParser.Schema_declarationContext):
        pass


    # Enter a parse tree produced by TSqlParser#column_declaration.
    def enterColumn_declaration(self, ctx:TSqlParser.Column_declarationContext):
        pass

    # Exit a parse tree produced by TSqlParser#column_declaration.
    def exitColumn_declaration(self, ctx:TSqlParser.Column_declarationContext):
        pass


    # Enter a parse tree produced by TSqlParser#change_table.
    def enterChange_table(self, ctx:TSqlParser.Change_tableContext):
        pass

    # Exit a parse tree produced by TSqlParser#change_table.
    def exitChange_table(self, ctx:TSqlParser.Change_tableContext):
        pass


    # Enter a parse tree produced by TSqlParser#join_part.
    def enterJoin_part(self, ctx:TSqlParser.Join_partContext):
        pass

    # Exit a parse tree produced by TSqlParser#join_part.
    def exitJoin_part(self, ctx:TSqlParser.Join_partContext):
        pass


    # Enter a parse tree produced by TSqlParser#pivot_clause.
    def enterPivot_clause(self, ctx:TSqlParser.Pivot_clauseContext):
        pass

    # Exit a parse tree produced by TSqlParser#pivot_clause.
    def exitPivot_clause(self, ctx:TSqlParser.Pivot_clauseContext):
        pass


    # Enter a parse tree produced by TSqlParser#unpivot_clause.
    def enterUnpivot_clause(self, ctx:TSqlParser.Unpivot_clauseContext):
        pass

    # Exit a parse tree produced by TSqlParser#unpivot_clause.
    def exitUnpivot_clause(self, ctx:TSqlParser.Unpivot_clauseContext):
        pass


    # Enter a parse tree produced by TSqlParser#full_column_name_list.
    def enterFull_column_name_list(self, ctx:TSqlParser.Full_column_name_listContext):
        pass

    # Exit a parse tree produced by TSqlParser#full_column_name_list.
    def exitFull_column_name_list(self, ctx:TSqlParser.Full_column_name_listContext):
        pass


    # Enter a parse tree produced by TSqlParser#table_name_with_hint.
    def enterTable_name_with_hint(self, ctx:TSqlParser.Table_name_with_hintContext):
        pass

    # Exit a parse tree produced by TSqlParser#table_name_with_hint.
    def exitTable_name_with_hint(self, ctx:TSqlParser.Table_name_with_hintContext):
        pass


    # Enter a parse tree produced by TSqlParser#rowset_function.
    def enterRowset_function(self, ctx:TSqlParser.Rowset_functionContext):
        pass

    # Exit a parse tree produced by TSqlParser#rowset_function.
    def exitRowset_function(self, ctx:TSqlParser.Rowset_functionContext):
        pass


    # Enter a parse tree produced by TSqlParser#bulk_option.
    def enterBulk_option(self, ctx:TSqlParser.Bulk_optionContext):
        pass

    # Exit a parse tree produced by TSqlParser#bulk_option.
    def exitBulk_option(self, ctx:TSqlParser.Bulk_optionContext):
        pass


    # Enter a parse tree produced by TSqlParser#derived_table.
    def enterDerived_table(self, ctx:TSqlParser.Derived_tableContext):
        pass

    # Exit a parse tree produced by TSqlParser#derived_table.
    def exitDerived_table(self, ctx:TSqlParser.Derived_tableContext):
        pass


    # Enter a parse tree produced by TSqlParser#RANKING_WINDOWED_FUNC.
    def enterRANKING_WINDOWED_FUNC(self, ctx:TSqlParser.RANKING_WINDOWED_FUNCContext):
        pass

    # Exit a parse tree produced by TSqlParser#RANKING_WINDOWED_FUNC.
    def exitRANKING_WINDOWED_FUNC(self, ctx:TSqlParser.RANKING_WINDOWED_FUNCContext):
        pass


    # Enter a parse tree produced by TSqlParser#AGGREGATE_WINDOWED_FUNC.
    def enterAGGREGATE_WINDOWED_FUNC(self, ctx:TSqlParser.AGGREGATE_WINDOWED_FUNCContext):
        pass

    # Exit a parse tree produced by TSqlParser#AGGREGATE_WINDOWED_FUNC.
    def exitAGGREGATE_WINDOWED_FUNC(self, ctx:TSqlParser.AGGREGATE_WINDOWED_FUNCContext):
        pass


    # Enter a parse tree produced by TSqlParser#ANALYTIC_WINDOWED_FUNC.
    def enterANALYTIC_WINDOWED_FUNC(self, ctx:TSqlParser.ANALYTIC_WINDOWED_FUNCContext):
        pass

    # Exit a parse tree produced by TSqlParser#ANALYTIC_WINDOWED_FUNC.
    def exitANALYTIC_WINDOWED_FUNC(self, ctx:TSqlParser.ANALYTIC_WINDOWED_FUNCContext):
        pass


    # Enter a parse tree produced by TSqlParser#SCALAR_FUNCTION.
    def enterSCALAR_FUNCTION(self, ctx:TSqlParser.SCALAR_FUNCTIONContext):
        pass

    # Exit a parse tree produced by TSqlParser#SCALAR_FUNCTION.
    def exitSCALAR_FUNCTION(self, ctx:TSqlParser.SCALAR_FUNCTIONContext):
        pass


    # Enter a parse tree produced by TSqlParser#BINARY_CHECKSUM.
    def enterBINARY_CHECKSUM(self, ctx:TSqlParser.BINARY_CHECKSUMContext):
        pass

    # Exit a parse tree produced by TSqlParser#BINARY_CHECKSUM.
    def exitBINARY_CHECKSUM(self, ctx:TSqlParser.BINARY_CHECKSUMContext):
        pass


    # Enter a parse tree produced by TSqlParser#CAST.
    def enterCAST(self, ctx:TSqlParser.CASTContext):
        pass

    # Exit a parse tree produced by TSqlParser#CAST.
    def exitCAST(self, ctx:TSqlParser.CASTContext):
        pass


    # Enter a parse tree produced by TSqlParser#CONVERT.
    def enterCONVERT(self, ctx:TSqlParser.CONVERTContext):
        pass

    # Exit a parse tree produced by TSqlParser#CONVERT.
    def exitCONVERT(self, ctx:TSqlParser.CONVERTContext):
        pass


    # Enter a parse tree produced by TSqlParser#CHECKSUM.
    def enterCHECKSUM(self, ctx:TSqlParser.CHECKSUMContext):
        pass

    # Exit a parse tree produced by TSqlParser#CHECKSUM.
    def exitCHECKSUM(self, ctx:TSqlParser.CHECKSUMContext):
        pass


    # Enter a parse tree produced by TSqlParser#COALESCE.
    def enterCOALESCE(self, ctx:TSqlParser.COALESCEContext):
        pass

    # Exit a parse tree produced by TSqlParser#COALESCE.
    def exitCOALESCE(self, ctx:TSqlParser.COALESCEContext):
        pass


    # Enter a parse tree produced by TSqlParser#CURRENT_TIMESTAMP.
    def enterCURRENT_TIMESTAMP(self, ctx:TSqlParser.CURRENT_TIMESTAMPContext):
        pass

    # Exit a parse tree produced by TSqlParser#CURRENT_TIMESTAMP.
    def exitCURRENT_TIMESTAMP(self, ctx:TSqlParser.CURRENT_TIMESTAMPContext):
        pass


    # Enter a parse tree produced by TSqlParser#CURRENT_USER.
    def enterCURRENT_USER(self, ctx:TSqlParser.CURRENT_USERContext):
        pass

    # Exit a parse tree produced by TSqlParser#CURRENT_USER.
    def exitCURRENT_USER(self, ctx:TSqlParser.CURRENT_USERContext):
        pass


    # Enter a parse tree produced by TSqlParser#DATEADD.
    def enterDATEADD(self, ctx:TSqlParser.DATEADDContext):
        pass

    # Exit a parse tree produced by TSqlParser#DATEADD.
    def exitDATEADD(self, ctx:TSqlParser.DATEADDContext):
        pass


    # Enter a parse tree produced by TSqlParser#DATEDIFF.
    def enterDATEDIFF(self, ctx:TSqlParser.DATEDIFFContext):
        pass

    # Exit a parse tree produced by TSqlParser#DATEDIFF.
    def exitDATEDIFF(self, ctx:TSqlParser.DATEDIFFContext):
        pass


    # Enter a parse tree produced by TSqlParser#DATENAME.
    def enterDATENAME(self, ctx:TSqlParser.DATENAMEContext):
        pass

    # Exit a parse tree produced by TSqlParser#DATENAME.
    def exitDATENAME(self, ctx:TSqlParser.DATENAMEContext):
        pass


    # Enter a parse tree produced by TSqlParser#DATEPART.
    def enterDATEPART(self, ctx:TSqlParser.DATEPARTContext):
        pass

    # Exit a parse tree produced by TSqlParser#DATEPART.
    def exitDATEPART(self, ctx:TSqlParser.DATEPARTContext):
        pass


    # Enter a parse tree produced by TSqlParser#GETDATE.
    def enterGETDATE(self, ctx:TSqlParser.GETDATEContext):
        pass

    # Exit a parse tree produced by TSqlParser#GETDATE.
    def exitGETDATE(self, ctx:TSqlParser.GETDATEContext):
        pass


    # Enter a parse tree produced by TSqlParser#GETUTCDATE.
    def enterGETUTCDATE(self, ctx:TSqlParser.GETUTCDATEContext):
        pass

    # Exit a parse tree produced by TSqlParser#GETUTCDATE.
    def exitGETUTCDATE(self, ctx:TSqlParser.GETUTCDATEContext):
        pass


    # Enter a parse tree produced by TSqlParser#IDENTITY.
    def enterIDENTITY(self, ctx:TSqlParser.IDENTITYContext):
        pass

    # Exit a parse tree produced by TSqlParser#IDENTITY.
    def exitIDENTITY(self, ctx:TSqlParser.IDENTITYContext):
        pass


    # Enter a parse tree produced by TSqlParser#MIN_ACTIVE_ROWVERSION.
    def enterMIN_ACTIVE_ROWVERSION(self, ctx:TSqlParser.MIN_ACTIVE_ROWVERSIONContext):
        pass

    # Exit a parse tree produced by TSqlParser#MIN_ACTIVE_ROWVERSION.
    def exitMIN_ACTIVE_ROWVERSION(self, ctx:TSqlParser.MIN_ACTIVE_ROWVERSIONContext):
        pass


    # Enter a parse tree produced by TSqlParser#NULLIF.
    def enterNULLIF(self, ctx:TSqlParser.NULLIFContext):
        pass

    # Exit a parse tree produced by TSqlParser#NULLIF.
    def exitNULLIF(self, ctx:TSqlParser.NULLIFContext):
        pass


    # Enter a parse tree produced by TSqlParser#STUFF.
    def enterSTUFF(self, ctx:TSqlParser.STUFFContext):
        pass

    # Exit a parse tree produced by TSqlParser#STUFF.
    def exitSTUFF(self, ctx:TSqlParser.STUFFContext):
        pass


    # Enter a parse tree produced by TSqlParser#SESSION_USER.
    def enterSESSION_USER(self, ctx:TSqlParser.SESSION_USERContext):
        pass

    # Exit a parse tree produced by TSqlParser#SESSION_USER.
    def exitSESSION_USER(self, ctx:TSqlParser.SESSION_USERContext):
        pass


    # Enter a parse tree produced by TSqlParser#SYSTEM_USER.
    def enterSYSTEM_USER(self, ctx:TSqlParser.SYSTEM_USERContext):
        pass

    # Exit a parse tree produced by TSqlParser#SYSTEM_USER.
    def exitSYSTEM_USER(self, ctx:TSqlParser.SYSTEM_USERContext):
        pass


    # Enter a parse tree produced by TSqlParser#ISNULL.
    def enterISNULL(self, ctx:TSqlParser.ISNULLContext):
        pass

    # Exit a parse tree produced by TSqlParser#ISNULL.
    def exitISNULL(self, ctx:TSqlParser.ISNULLContext):
        pass


    # Enter a parse tree produced by TSqlParser#XML_DATA_TYPE_FUNC.
    def enterXML_DATA_TYPE_FUNC(self, ctx:TSqlParser.XML_DATA_TYPE_FUNCContext):
        pass

    # Exit a parse tree produced by TSqlParser#XML_DATA_TYPE_FUNC.
    def exitXML_DATA_TYPE_FUNC(self, ctx:TSqlParser.XML_DATA_TYPE_FUNCContext):
        pass


    # Enter a parse tree produced by TSqlParser#IFF.
    def enterIFF(self, ctx:TSqlParser.IFFContext):
        pass

    # Exit a parse tree produced by TSqlParser#IFF.
    def exitIFF(self, ctx:TSqlParser.IFFContext):
        pass


    # Enter a parse tree produced by TSqlParser#xml_data_type_methods.
    def enterXml_data_type_methods(self, ctx:TSqlParser.Xml_data_type_methodsContext):
        pass

    # Exit a parse tree produced by TSqlParser#xml_data_type_methods.
    def exitXml_data_type_methods(self, ctx:TSqlParser.Xml_data_type_methodsContext):
        pass


    # Enter a parse tree produced by TSqlParser#value_method.
    def enterValue_method(self, ctx:TSqlParser.Value_methodContext):
        pass

    # Exit a parse tree produced by TSqlParser#value_method.
    def exitValue_method(self, ctx:TSqlParser.Value_methodContext):
        pass


    # Enter a parse tree produced by TSqlParser#query_method.
    def enterQuery_method(self, ctx:TSqlParser.Query_methodContext):
        pass

    # Exit a parse tree produced by TSqlParser#query_method.
    def exitQuery_method(self, ctx:TSqlParser.Query_methodContext):
        pass


    # Enter a parse tree produced by TSqlParser#exist_method.
    def enterExist_method(self, ctx:TSqlParser.Exist_methodContext):
        pass

    # Exit a parse tree produced by TSqlParser#exist_method.
    def exitExist_method(self, ctx:TSqlParser.Exist_methodContext):
        pass


    # Enter a parse tree produced by TSqlParser#modify_method.
    def enterModify_method(self, ctx:TSqlParser.Modify_methodContext):
        pass

    # Exit a parse tree produced by TSqlParser#modify_method.
    def exitModify_method(self, ctx:TSqlParser.Modify_methodContext):
        pass


    # Enter a parse tree produced by TSqlParser#nodes_method.
    def enterNodes_method(self, ctx:TSqlParser.Nodes_methodContext):
        pass

    # Exit a parse tree produced by TSqlParser#nodes_method.
    def exitNodes_method(self, ctx:TSqlParser.Nodes_methodContext):
        pass


    # Enter a parse tree produced by TSqlParser#switch_section.
    def enterSwitch_section(self, ctx:TSqlParser.Switch_sectionContext):
        pass

    # Exit a parse tree produced by TSqlParser#switch_section.
    def exitSwitch_section(self, ctx:TSqlParser.Switch_sectionContext):
        pass


    # Enter a parse tree produced by TSqlParser#switch_search_condition_section.
    def enterSwitch_search_condition_section(self, ctx:TSqlParser.Switch_search_condition_sectionContext):
        pass

    # Exit a parse tree produced by TSqlParser#switch_search_condition_section.
    def exitSwitch_search_condition_section(self, ctx:TSqlParser.Switch_search_condition_sectionContext):
        pass


    # Enter a parse tree produced by TSqlParser#as_column_alias.
    def enterAs_column_alias(self, ctx:TSqlParser.As_column_aliasContext):
        pass

    # Exit a parse tree produced by TSqlParser#as_column_alias.
    def exitAs_column_alias(self, ctx:TSqlParser.As_column_aliasContext):
        pass


    # Enter a parse tree produced by TSqlParser#as_table_alias.
    def enterAs_table_alias(self, ctx:TSqlParser.As_table_aliasContext):
        pass

    # Exit a parse tree produced by TSqlParser#as_table_alias.
    def exitAs_table_alias(self, ctx:TSqlParser.As_table_aliasContext):
        pass


    # Enter a parse tree produced by TSqlParser#table_alias.
    def enterTable_alias(self, ctx:TSqlParser.Table_aliasContext):
        pass

    # Exit a parse tree produced by TSqlParser#table_alias.
    def exitTable_alias(self, ctx:TSqlParser.Table_aliasContext):
        pass


    # Enter a parse tree produced by TSqlParser#with_table_hints.
    def enterWith_table_hints(self, ctx:TSqlParser.With_table_hintsContext):
        pass

    # Exit a parse tree produced by TSqlParser#with_table_hints.
    def exitWith_table_hints(self, ctx:TSqlParser.With_table_hintsContext):
        pass


    # Enter a parse tree produced by TSqlParser#insert_with_table_hints.
    def enterInsert_with_table_hints(self, ctx:TSqlParser.Insert_with_table_hintsContext):
        pass

    # Exit a parse tree produced by TSqlParser#insert_with_table_hints.
    def exitInsert_with_table_hints(self, ctx:TSqlParser.Insert_with_table_hintsContext):
        pass


    # Enter a parse tree produced by TSqlParser#table_hint.
    def enterTable_hint(self, ctx:TSqlParser.Table_hintContext):
        pass

    # Exit a parse tree produced by TSqlParser#table_hint.
    def exitTable_hint(self, ctx:TSqlParser.Table_hintContext):
        pass


    # Enter a parse tree produced by TSqlParser#index_value.
    def enterIndex_value(self, ctx:TSqlParser.Index_valueContext):
        pass

    # Exit a parse tree produced by TSqlParser#index_value.
    def exitIndex_value(self, ctx:TSqlParser.Index_valueContext):
        pass


    # Enter a parse tree produced by TSqlParser#column_alias_list.
    def enterColumn_alias_list(self, ctx:TSqlParser.Column_alias_listContext):
        pass

    # Exit a parse tree produced by TSqlParser#column_alias_list.
    def exitColumn_alias_list(self, ctx:TSqlParser.Column_alias_listContext):
        pass


    # Enter a parse tree produced by TSqlParser#column_alias.
    def enterColumn_alias(self, ctx:TSqlParser.Column_aliasContext):
        pass

    # Exit a parse tree produced by TSqlParser#column_alias.
    def exitColumn_alias(self, ctx:TSqlParser.Column_aliasContext):
        pass


    # Enter a parse tree produced by TSqlParser#table_value_constructor.
    def enterTable_value_constructor(self, ctx:TSqlParser.Table_value_constructorContext):
        pass

    # Exit a parse tree produced by TSqlParser#table_value_constructor.
    def exitTable_value_constructor(self, ctx:TSqlParser.Table_value_constructorContext):
        pass


    # Enter a parse tree produced by TSqlParser#expression_list.
    def enterExpression_list(self, ctx:TSqlParser.Expression_listContext):
        pass

    # Exit a parse tree produced by TSqlParser#expression_list.
    def exitExpression_list(self, ctx:TSqlParser.Expression_listContext):
        pass


    # Enter a parse tree produced by TSqlParser#ranking_windowed_function.
    def enterRanking_windowed_function(self, ctx:TSqlParser.Ranking_windowed_functionContext):
        pass

    # Exit a parse tree produced by TSqlParser#ranking_windowed_function.
    def exitRanking_windowed_function(self, ctx:TSqlParser.Ranking_windowed_functionContext):
        pass


    # Enter a parse tree produced by TSqlParser#aggregate_windowed_function.
    def enterAggregate_windowed_function(self, ctx:TSqlParser.Aggregate_windowed_functionContext):
        pass

    # Exit a parse tree produced by TSqlParser#aggregate_windowed_function.
    def exitAggregate_windowed_function(self, ctx:TSqlParser.Aggregate_windowed_functionContext):
        pass


    # Enter a parse tree produced by TSqlParser#analytic_windowed_function.
    def enterAnalytic_windowed_function(self, ctx:TSqlParser.Analytic_windowed_functionContext):
        pass

    # Exit a parse tree produced by TSqlParser#analytic_windowed_function.
    def exitAnalytic_windowed_function(self, ctx:TSqlParser.Analytic_windowed_functionContext):
        pass


    # Enter a parse tree produced by TSqlParser#all_distinct_expression.
    def enterAll_distinct_expression(self, ctx:TSqlParser.All_distinct_expressionContext):
        pass

    # Exit a parse tree produced by TSqlParser#all_distinct_expression.
    def exitAll_distinct_expression(self, ctx:TSqlParser.All_distinct_expressionContext):
        pass


    # Enter a parse tree produced by TSqlParser#over_clause.
    def enterOver_clause(self, ctx:TSqlParser.Over_clauseContext):
        pass

    # Exit a parse tree produced by TSqlParser#over_clause.
    def exitOver_clause(self, ctx:TSqlParser.Over_clauseContext):
        pass


    # Enter a parse tree produced by TSqlParser#row_or_range_clause.
    def enterRow_or_range_clause(self, ctx:TSqlParser.Row_or_range_clauseContext):
        pass

    # Exit a parse tree produced by TSqlParser#row_or_range_clause.
    def exitRow_or_range_clause(self, ctx:TSqlParser.Row_or_range_clauseContext):
        pass


    # Enter a parse tree produced by TSqlParser#window_frame_extent.
    def enterWindow_frame_extent(self, ctx:TSqlParser.Window_frame_extentContext):
        pass

    # Exit a parse tree produced by TSqlParser#window_frame_extent.
    def exitWindow_frame_extent(self, ctx:TSqlParser.Window_frame_extentContext):
        pass


    # Enter a parse tree produced by TSqlParser#window_frame_bound.
    def enterWindow_frame_bound(self, ctx:TSqlParser.Window_frame_boundContext):
        pass

    # Exit a parse tree produced by TSqlParser#window_frame_bound.
    def exitWindow_frame_bound(self, ctx:TSqlParser.Window_frame_boundContext):
        pass


    # Enter a parse tree produced by TSqlParser#window_frame_preceding.
    def enterWindow_frame_preceding(self, ctx:TSqlParser.Window_frame_precedingContext):
        pass

    # Exit a parse tree produced by TSqlParser#window_frame_preceding.
    def exitWindow_frame_preceding(self, ctx:TSqlParser.Window_frame_precedingContext):
        pass


    # Enter a parse tree produced by TSqlParser#window_frame_following.
    def enterWindow_frame_following(self, ctx:TSqlParser.Window_frame_followingContext):
        pass

    # Exit a parse tree produced by TSqlParser#window_frame_following.
    def exitWindow_frame_following(self, ctx:TSqlParser.Window_frame_followingContext):
        pass


    # Enter a parse tree produced by TSqlParser#full_table_name.
    def enterFull_table_name(self, ctx:TSqlParser.Full_table_nameContext):
        pass

    # Exit a parse tree produced by TSqlParser#full_table_name.
    def exitFull_table_name(self, ctx:TSqlParser.Full_table_nameContext):
        pass


    # Enter a parse tree produced by TSqlParser#table_name.
    def enterTable_name(self, ctx:TSqlParser.Table_nameContext):
        pass

    # Exit a parse tree produced by TSqlParser#table_name.
    def exitTable_name(self, ctx:TSqlParser.Table_nameContext):
        pass


    # Enter a parse tree produced by TSqlParser#func_proc_name_schema.
    def enterFunc_proc_name_schema(self, ctx:TSqlParser.Func_proc_name_schemaContext):
        pass

    # Exit a parse tree produced by TSqlParser#func_proc_name_schema.
    def exitFunc_proc_name_schema(self, ctx:TSqlParser.Func_proc_name_schemaContext):
        pass


    # Enter a parse tree produced by TSqlParser#func_proc_name_database_schema.
    def enterFunc_proc_name_database_schema(self, ctx:TSqlParser.Func_proc_name_database_schemaContext):
        pass

    # Exit a parse tree produced by TSqlParser#func_proc_name_database_schema.
    def exitFunc_proc_name_database_schema(self, ctx:TSqlParser.Func_proc_name_database_schemaContext):
        pass


    # Enter a parse tree produced by TSqlParser#func_proc_name_server_database_schema.
    def enterFunc_proc_name_server_database_schema(self, ctx:TSqlParser.Func_proc_name_server_database_schemaContext):
        pass

    # Exit a parse tree produced by TSqlParser#func_proc_name_server_database_schema.
    def exitFunc_proc_name_server_database_schema(self, ctx:TSqlParser.Func_proc_name_server_database_schemaContext):
        pass


    # Enter a parse tree produced by TSqlParser#ddl_object.
    def enterDdl_object(self, ctx:TSqlParser.Ddl_objectContext):
        pass

    # Exit a parse tree produced by TSqlParser#ddl_object.
    def exitDdl_object(self, ctx:TSqlParser.Ddl_objectContext):
        pass


    # Enter a parse tree produced by TSqlParser#full_column_name.
    def enterFull_column_name(self, ctx:TSqlParser.Full_column_nameContext):
        pass

    # Exit a parse tree produced by TSqlParser#full_column_name.
    def exitFull_column_name(self, ctx:TSqlParser.Full_column_nameContext):
        pass


    # Enter a parse tree produced by TSqlParser#column_name_list_with_order.
    def enterColumn_name_list_with_order(self, ctx:TSqlParser.Column_name_list_with_orderContext):
        pass

    # Exit a parse tree produced by TSqlParser#column_name_list_with_order.
    def exitColumn_name_list_with_order(self, ctx:TSqlParser.Column_name_list_with_orderContext):
        pass


    # Enter a parse tree produced by TSqlParser#column_name_list.
    def enterColumn_name_list(self, ctx:TSqlParser.Column_name_listContext):
        pass

    # Exit a parse tree produced by TSqlParser#column_name_list.
    def exitColumn_name_list(self, ctx:TSqlParser.Column_name_listContext):
        pass


    # Enter a parse tree produced by TSqlParser#cursor_name.
    def enterCursor_name(self, ctx:TSqlParser.Cursor_nameContext):
        pass

    # Exit a parse tree produced by TSqlParser#cursor_name.
    def exitCursor_name(self, ctx:TSqlParser.Cursor_nameContext):
        pass


    # Enter a parse tree produced by TSqlParser#on_off.
    def enterOn_off(self, ctx:TSqlParser.On_offContext):
        pass

    # Exit a parse tree produced by TSqlParser#on_off.
    def exitOn_off(self, ctx:TSqlParser.On_offContext):
        pass


    # Enter a parse tree produced by TSqlParser#clustered.
    def enterClustered(self, ctx:TSqlParser.ClusteredContext):
        pass

    # Exit a parse tree produced by TSqlParser#clustered.
    def exitClustered(self, ctx:TSqlParser.ClusteredContext):
        pass


    # Enter a parse tree produced by TSqlParser#null_notnull.
    def enterNull_notnull(self, ctx:TSqlParser.Null_notnullContext):
        pass

    # Exit a parse tree produced by TSqlParser#null_notnull.
    def exitNull_notnull(self, ctx:TSqlParser.Null_notnullContext):
        pass


    # Enter a parse tree produced by TSqlParser#null_or_default.
    def enterNull_or_default(self, ctx:TSqlParser.Null_or_defaultContext):
        pass

    # Exit a parse tree produced by TSqlParser#null_or_default.
    def exitNull_or_default(self, ctx:TSqlParser.Null_or_defaultContext):
        pass


    # Enter a parse tree produced by TSqlParser#scalar_function_name.
    def enterScalar_function_name(self, ctx:TSqlParser.Scalar_function_nameContext):
        pass

    # Exit a parse tree produced by TSqlParser#scalar_function_name.
    def exitScalar_function_name(self, ctx:TSqlParser.Scalar_function_nameContext):
        pass


    # Enter a parse tree produced by TSqlParser#begin_conversation_timer.
    def enterBegin_conversation_timer(self, ctx:TSqlParser.Begin_conversation_timerContext):
        pass

    # Exit a parse tree produced by TSqlParser#begin_conversation_timer.
    def exitBegin_conversation_timer(self, ctx:TSqlParser.Begin_conversation_timerContext):
        pass


    # Enter a parse tree produced by TSqlParser#begin_conversation_dialog.
    def enterBegin_conversation_dialog(self, ctx:TSqlParser.Begin_conversation_dialogContext):
        pass

    # Exit a parse tree produced by TSqlParser#begin_conversation_dialog.
    def exitBegin_conversation_dialog(self, ctx:TSqlParser.Begin_conversation_dialogContext):
        pass


    # Enter a parse tree produced by TSqlParser#contract_name.
    def enterContract_name(self, ctx:TSqlParser.Contract_nameContext):
        pass

    # Exit a parse tree produced by TSqlParser#contract_name.
    def exitContract_name(self, ctx:TSqlParser.Contract_nameContext):
        pass


    # Enter a parse tree produced by TSqlParser#service_name.
    def enterService_name(self, ctx:TSqlParser.Service_nameContext):
        pass

    # Exit a parse tree produced by TSqlParser#service_name.
    def exitService_name(self, ctx:TSqlParser.Service_nameContext):
        pass


    # Enter a parse tree produced by TSqlParser#end_conversation.
    def enterEnd_conversation(self, ctx:TSqlParser.End_conversationContext):
        pass

    # Exit a parse tree produced by TSqlParser#end_conversation.
    def exitEnd_conversation(self, ctx:TSqlParser.End_conversationContext):
        pass


    # Enter a parse tree produced by TSqlParser#waitfor_conversation.
    def enterWaitfor_conversation(self, ctx:TSqlParser.Waitfor_conversationContext):
        pass

    # Exit a parse tree produced by TSqlParser#waitfor_conversation.
    def exitWaitfor_conversation(self, ctx:TSqlParser.Waitfor_conversationContext):
        pass


    # Enter a parse tree produced by TSqlParser#get_conversation.
    def enterGet_conversation(self, ctx:TSqlParser.Get_conversationContext):
        pass

    # Exit a parse tree produced by TSqlParser#get_conversation.
    def exitGet_conversation(self, ctx:TSqlParser.Get_conversationContext):
        pass


    # Enter a parse tree produced by TSqlParser#queue_id.
    def enterQueue_id(self, ctx:TSqlParser.Queue_idContext):
        pass

    # Exit a parse tree produced by TSqlParser#queue_id.
    def exitQueue_id(self, ctx:TSqlParser.Queue_idContext):
        pass


    # Enter a parse tree produced by TSqlParser#send_conversation.
    def enterSend_conversation(self, ctx:TSqlParser.Send_conversationContext):
        pass

    # Exit a parse tree produced by TSqlParser#send_conversation.
    def exitSend_conversation(self, ctx:TSqlParser.Send_conversationContext):
        pass


    # Enter a parse tree produced by TSqlParser#data_type.
    def enterData_type(self, ctx:TSqlParser.Data_typeContext):
        pass

    # Exit a parse tree produced by TSqlParser#data_type.
    def exitData_type(self, ctx:TSqlParser.Data_typeContext):
        pass


    # Enter a parse tree produced by TSqlParser#constant.
    def enterConstant(self, ctx:TSqlParser.ConstantContext):
        pass

    # Exit a parse tree produced by TSqlParser#constant.
    def exitConstant(self, ctx:TSqlParser.ConstantContext):
        pass


    # Enter a parse tree produced by TSqlParser#sign.
    def enterSign(self, ctx:TSqlParser.SignContext):
        pass

    # Exit a parse tree produced by TSqlParser#sign.
    def exitSign(self, ctx:TSqlParser.SignContext):
        pass


    # Enter a parse tree produced by TSqlParser#identifier.
    def enterIdentifier(self, ctx:TSqlParser.IdentifierContext):
        pass

    # Exit a parse tree produced by TSqlParser#identifier.
    def exitIdentifier(self, ctx:TSqlParser.IdentifierContext):
        pass


    # Enter a parse tree produced by TSqlParser#simple_id.
    def enterSimple_id(self, ctx:TSqlParser.Simple_idContext):
        pass

    # Exit a parse tree produced by TSqlParser#simple_id.
    def exitSimple_id(self, ctx:TSqlParser.Simple_idContext):
        pass


    # Enter a parse tree produced by TSqlParser#comparison_operator.
    def enterComparison_operator(self, ctx:TSqlParser.Comparison_operatorContext):
        pass

    # Exit a parse tree produced by TSqlParser#comparison_operator.
    def exitComparison_operator(self, ctx:TSqlParser.Comparison_operatorContext):
        pass


    # Enter a parse tree produced by TSqlParser#assignment_operator.
    def enterAssignment_operator(self, ctx:TSqlParser.Assignment_operatorContext):
        pass

    # Exit a parse tree produced by TSqlParser#assignment_operator.
    def exitAssignment_operator(self, ctx:TSqlParser.Assignment_operatorContext):
        pass


