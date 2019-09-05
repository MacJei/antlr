from antlr4 import *
from Teradata.parser.TDantlrLexer import TDantlrLexer
from Teradata.parser.TDantlrListener import TDantlrListener
from Teradata.parser.TDantlrParser import TDantlrParser
from antlr4.tree.Tree import TerminalNodeImpl
import Teradata.parser.Tera2Spark as Tera2Spark
import datetime
import sys
import re


class PySparkParse(TDantlrListener):
    def __init__(self, tokens:CommonTokenStream):
        #function
        self.fn_nest_cnt = 0
        self.fn_tokens = []
        self.fn_spaces = []
        self.fn_conv_txt = {}        
        
        #alias
        self.alias_scope = ''
        self.alias_scope_txt = {}
        self.alias_order_by = False  
        
        #interval
        self.intv_nest_cnt = 0
        self.intv_tokens = []
        self.intv_spaces = []
        self.intv_conv_txt = {}

        #field mode cast
        self.fld_mode_cast_start = 0
        self.fld_mode_cast_end = 0
        self.fld_mode_cast = False
        #self.fld_mode_cast_txt = {}

        #concat
        self.cnct_nest_cnt = 0
        self.cnct_tokens = []
        self.cnct_spaces = []
        self.cnct_conv_txt = {}

        #case specific
        self.case_spf = False

        #qualify
        self.qlfy_expr = False
        self.qlfy_str = ''
        self.qlfy_scope = 0
        self.qlfy_scope_stat = []
        self.qlfy_sub_txt = {}
        self.qlfy_al_cntr = 0

        #common
        self.token_stream = tokens.tokens
        self.out_sql = ''


##function##
    def enterExprRule12(self, ctx:TDantlrParser.ExprRule12Context):
        self.enterFunction(ctx)

    def exitExprRule12(self, ctx:TDantlrParser.ExprRule12Context):
        self.exitFunction(ctx)

    def enterExprRule15(self, ctx:TDantlrParser.ExprRule15Context):
        self.enterFunction(ctx)

    def exitExprRule15(self, ctx:TDantlrParser.ExprRule15Context):
        self.exitFunction(ctx)

    def enterFunction(self, ctx):
        self.fn_nest_cnt += 1

    def exitFunction(self, ctx):
        self.fn_nest_cnt -= 1

        self.fn_tokens = []
        self.fn_spaces = []
        self.walkFunctionExpr(ctx)

        fn_new_str = Tera2Spark.convert(self.fn_tokens, self.fn_spaces)

        if self.fn_nest_cnt == 0 and self.intv_nest_cnt == 0 and self.cnct_nest_cnt == 0 and not(self.qlfy_expr):
            self.out_sql += fn_new_str
            self.fn_conv_txt[str(ctx.getSourceInterval()[0])] = fn_new_str.strip()
        else:
            self.fn_conv_txt[str(ctx.getSourceInterval()[0])] = fn_new_str.strip()

    def walkFunctionExpr(self, ctx):
        for child in ctx.getChildren():
            if isinstance(child,TerminalNodeImpl):

                tok_txt = child.getText()
                #replace alias with expr
                if isinstance(child.parentCtx, TDantlrParser.Column_nameContext) and not(self.alias_order_by) and tok_txt in self.alias_scope_txt[self.alias_scope].keys():
                    tok_txt = self.alias_scope_txt[self.alias_scope][tok_txt]

                #logic to remove extra parenthesis
                #check if expr in parenthesis
                if isinstance(child.parentCtx, TDantlrParser.ExprRule14Context):
                    if isinstance(child.parentCtx.getChild(1).getChild(2), TDantlrParser.ExprRule06Context):
                        pass
                    else:
                        self.fn_tokens.append(tok_txt)
                        hdn_tok = self.getLeftHiddenToken(child)
                        self.fn_spaces.append(hdn_tok)
                else:
                    self.fn_tokens.append(tok_txt)
                    hdn_tok = self.getLeftHiddenToken(child)
                    self.fn_spaces.append(hdn_tok)

            #check if expr is function or cast
            elif isinstance(child, TDantlrParser.ExprRule12Context) or isinstance(child, TDantlrParser.ExprRule15Context):
                self.fn_tokens.append(self.fn_conv_txt[str(child.getSourceInterval()[0])])
                hdn_tok = self.getLeftHiddenToken(child)
                self.fn_spaces.append(hdn_tok)

            #check if expr is concat
            elif isinstance(child, TDantlrParser.ExprRule07Context):
                self.fn_tokens.append(self.cnct_conv_txt[str(child.getSourceInterval()[0])])
                hdn_tok = self.getLeftHiddenToken(child)
                self.fn_spaces.append(hdn_tok)

            #check if expr is plus/minus interval
            elif isinstance(child, TDantlrParser.ExprRule08Context) and isinstance(child.getChild(2).getChild(0), TDantlrParser.Interval_exprContext):
                hdn_tok = self.getLeftHiddenToken(child)
                self.fn_spaces.append(hdn_tok)

            else:
                self.walkFunctionExpr(child)



##alias##

    def enterOrder_by_list(self, ctx:TDantlrParser.Order_by_listContext):
        self.alias_order_by = True

    def exitOrder_by_list(self, ctx:TDantlrParser.Order_by_listContext):
        self.alias_order_by = False

    def enterExpr_alias_name(self, ctx:TDantlrParser.Expr_alias_nameContext):
        alias = ctx.getText()
        if alias not in self.alias_scope_txt[self.alias_scope].keys():
            self.alias_scope_txt[self.alias_scope][alias]=''
            self.walkAliasExpr(ctx.parentCtx.getChild(0), alias)
            self.alias_scope_txt[self.alias_scope][alias] = '(' + self.alias_scope_txt[self.alias_scope][alias] + ')'

        alias_keys = list(self.alias_scope_txt[self.alias_scope].keys())
        for i in range(len(alias_keys)-1):
            for j in range(i+1,len(alias_keys)):
                k1 = alias_keys[i]
                v1 = self.alias_scope_txt[self.alias_scope][alias_keys[i]]
                k2 = alias_keys[j]
                v2 = self.alias_scope_txt[self.alias_scope][alias_keys[j]]

                if k1 in v2.strip('\(').strip('\)').split():
                    v2 = v2.replace(k1,v1)
                    self.alias_scope_txt[self.alias_scope][k2] = v2.strip()

    def walkAliasExpr(self, ctx, alias):
        #check if expr is function or cast
        if isinstance(ctx, TDantlrParser.ExprRule12Context) or isinstance(ctx, TDantlrParser.ExprRule15Context):
            alias_expr = self.fn_conv_txt[str(ctx.getSourceInterval()[0])]
            self.alias_scope_txt[self.alias_scope][alias] += alias_expr

        #check if expr is concat
        elif isinstance(ctx, TDantlrParser.ExprRule07Context):
            alias_expr = self.cnct_conv_txt[str(ctx.getSourceInterval()[0])]
            self.alias_scope_txt[self.alias_scope][alias] += alias_expr

        #check if expr is plus/minus interval
        elif isinstance(ctx, TDantlrParser.ExprRule08Context) and isinstance(ctx.getChild(2).getChild(0), TDantlrParser.Interval_exprContext):
            alias_expr = self.intv_conv_txt[str(ctx.getSourceInterval()[0])]
            self.alias_scope_txt[self.alias_scope][alias] += alias_expr

        else:
            for child in ctx.getChildren():
                if isinstance(child, TerminalNodeImpl):
                    tok_txt = child.getText()
                    hdn_tok = self.getLeftHiddenToken(child)

                    self.alias_scope_txt[self.alias_scope][alias] += hdn_tok + tok_txt
                    self.alias_scope_txt[self.alias_scope][alias] = self.alias_scope_txt[self.alias_scope][alias].strip()
                else:
                    self.walkAliasExpr(child, alias)


##interval##

    def enterExprRule08(self, ctx:TDantlrParser.ExprRule08Context):
        gr_child = ctx.getChild(2).getChild(0)
        if isinstance(gr_child, TDantlrParser.Interval_exprContext):
            self.intv_nest_cnt += 1

    def exitExprRule08(self, ctx:TDantlrParser.ExprRule08Context):
        gr_child = ctx.getChild(2).getChild(0)
        if isinstance(gr_child, TDantlrParser.Interval_exprContext):
            r_child_tokens = list(x.getText() for x in gr_child.getChildren())
            self.intv_nest_cnt -= 1

            op_txt = ctx.getChild(1).getText()

            l_child = ctx.getChild(0)
            l_child_hdn_tok = self.getLeftHiddenToken(l_child)

            if isinstance(l_child, TDantlrParser.ExprRule12Context) or isinstance(l_child, TDantlrParser.ExprRule15Context):
                l_child_txt = self.fn_conv_txt[str(l_child.getSourceInterval()[0])]

            else:
                self.intv_tokens = []
                self.intv_spaces = []
                self.walkIntervalLeftExpr(l_child)

                l_child_txt = ''.join(list(tkn[0] + tkn[1] for tkn in zip(self.intv_spaces,self.intv_tokens)))
                l_child_txt = l_child_txt.strip()

            tokens = r_child_tokens+[op_txt]+[l_child_txt]
            spaces = [l_child_hdn_tok,' ',' ',' ',' ']

            intv_new_str = Tera2Spark.convert(tokens, spaces)

            if self.fn_nest_cnt == 0 and self.intv_nest_cnt == 0 and self.cnct_nest_cnt == 0:
                self.out_sql += intv_new_str
                self.intv_conv_txt[str(ctx.getSourceInterval()[0])] = intv_new_str.strip()
            else:
                self.intv_conv_txt[str(ctx.getSourceInterval()[0])] = intv_new_str.strip()

    def walkIntervalLeftExpr(self, ctx):
        for child in ctx.getChildren():
            if isinstance(child,TerminalNodeImpl):

                tok_txt = child.getText()
                #replace alias with expr
                if isinstance(child.parentCtx, TDantlrParser.Column_nameContext) and not(self.alias_order_by) and tok_txt in self.alias_scope_txt[self.alias_scope].keys():
                    tok_txt = self.alias_scope_txt[self.alias_scope][tok_txt]

                #logic to remove extra parenthesis
                #check if expr in parenthesis
                if isinstance(child.parentCtx, TDantlrParser.ExprRule14Context):
                    if isinstance(child.parentCtx.getChild(1).getChild(2), TDantlrParser.ExprRule06Context):
                        pass
                    else:
                        self.intv_tokens.append(tok_txt)
                        hdn_tok = self.getLeftHiddenToken(child)
                        self.intv_spaces.append(hdn_tok)
                else:
                    self.intv_tokens.append(tok_txt)
                    hdn_tok = self.getLeftHiddenToken(child)
                    self.intv_spaces.append(hdn_tok)

            #check if expr is function or cast
            elif isinstance(child, TDantlrParser.ExprRule12Context) or isinstance(child, TDantlrParser.ExprRule15Context):
                self.intv_tokens.append(self.fn_conv_txt[str(str(child.getSourceInterval()[0]))])
                hdn_tok = self.getLeftHiddenToken(child)
                self.intv_spaces.append(hdn_tok)

            #check if expr is concat
            elif isinstance(child, TDantlrParser.ExprRule07Context):
                self.intv_tokens.append(self.cnct_conv_txt[str(child.getSourceInterval()[0])])
                hdn_tok = self.getLeftHiddenToken(child)
                self.intv_spaces.append(hdn_tok)

            #check if expr is plus/minus interval
            elif isinstance(child, TDantlrParser.ExprRule08Context) and isinstance(child.getChild(2).getChild(0), TDantlrParser.Interval_exprContext):
                self.intv_tokens.append(self.intv_conv_txt[str(child.getSourceInterval()[0])])
                hdn_tok = self.getLeftHiddenToken(child)
                self.intv_spaces.append(hdn_tok)

            else:
                self.walkIntervalLeftExpr(child)


##field mode cast##

    def enterExprRule22(self, ctx:TDantlrParser.ExprRule22Context):
        self.fld_mode_cast_start = len(self.out_sql)
        self.out_sql += ' cast[::]'

    def exitExprRule22(self, ctx:TDantlrParser.ExprRule22Context):
        r_child_txt = ctx.getChild(1).getText()[1:-1]

        if bool(re.match(r'TIMESTAMP\(\d+\)', r_child_txt, re.I)):
            r_child_txt = 'TIMESTAMP'

        self.out_sql += " as {})".format(r_child_txt)
        self.out_sql = re.sub(r'\scast\[::\](\s*)', r'\1cast(', self.out_sql, flags = re.I)

        self.fld_mode_cast_end = len(self.out_sql)

        #self.fld_mode_cast_txt[str(ctx.getSourceInterval()[0])] = self.out_sql[self.fld_mode_cast_start:self.fld_mode_cast_end]
        prnt_ctx = ctx.parentCtx
        if isinstance(prnt_ctx, TDantlrParser.Select_list_exprContext):
            chld_cnt = prnt_ctx.getChildCount()
            chld_ctx = prnt_ctx.getChild(chld_cnt - 1)
            if isinstance(chld_ctx, TDantlrParser.Expr_alias_nameContext):
                alias = chld_ctx.getText()
                self.alias_scope_txt[self.alias_scope][alias] = self.out_sql[self.fld_mode_cast_start:self.fld_mode_cast_end]

    def enterField_mode_cast(self, ctx:TDantlrParser.Field_mode_castContext):
        self.fld_mode_cast = True

    def exitField_mode_cast(self, ctx:TDantlrParser.Field_mode_castContext):
        self.fld_mode_cast = False


##concat##
    def enterExprRule07(self, ctx:TDantlrParser.ExprRule07Context):
        self.cnct_nest_cnt += 1

    def exitExprRule07(self, ctx:TDantlrParser.ExprRule07Context):
        self.cnct_nest_cnt -= 1

        l_child = ctx.getChild(0)
        if isinstance(l_child, TDantlrParser.ExprRule07Context):
            l_child_txt = self.cnct_conv_txt[str(l_child.getSourceInterval()[0])]
        else:
            self.cnct_tokens = []
            self.cnct_spaces = []
            self.walkConcatExprChild(l_child)
            l_child_txt = ''.join(list(tkn[0] + tkn[1] for tkn in zip(self.cnct_spaces,self.cnct_tokens)))

        r_child = ctx.getChild(2)
        self.cnct_tokens = []
        self.cnct_spaces = []
        self.walkConcatExprChild(r_child)
        r_child_txt = ''.join(list(tkn[0] + tkn[1] for tkn in zip(self.cnct_spaces,self.cnct_tokens)))

        cnct_new_str = 'concat[::]'+l_child_txt+','+r_child_txt+')'

        if self.fn_nest_cnt == 0 and self.intv_nest_cnt == 0 and self.cnct_nest_cnt == 0:
            cnct_wsp_rev = re.findall(r'concat\[::\](\s*)', cnct_new_str, re.I)[::-1]
            for wsp in cnct_wsp_rev:
                cnct_new_str = re.sub(r'\s*concat\[::\]\s*', wsp+'concat(', cnct_new_str, 1, flags = re.I)

            self.out_sql += cnct_new_str
            self.cnct_conv_txt[str(ctx.getSourceInterval()[0])] = cnct_new_str.strip()

        else:
            cnct_wsp_rev = re.findall(r'concat\[::\](\s*)', cnct_new_str, re.I)[::-1]
            for wsp in cnct_wsp_rev:
                cnct_new_str = re.sub(r'\s*concat\[::\]\s*', wsp+'concat(', cnct_new_str, 1, flags = re.I)
            self.cnct_conv_txt[str(ctx.getSourceInterval()[0])] = cnct_new_str


    def walkConcatExprChild(self, ctx):
        for child in ctx.getChildren():
            if isinstance(child,TerminalNodeImpl):

                tok_txt = child.getText()
                if isinstance(child.parentCtx, TDantlrParser.Column_nameContext) and not(self.alias_order_by) and tok_txt in self.alias_scope_txt[self.alias_scope].keys():
                    tok_txt = self.alias_scope_txt[self.alias_scope][tok_txt]

                #logic to remove extra parenthesis
                if isinstance(child.parentCtx, TDantlrParser.ExprRule14Context):
                    if isinstance(child.parentCtx.getChild(1).getChild(2), TDantlrParser.ExprRule06Context):
                        pass
                    else:
                        self.cnct_tokens.append(tok_txt)
                        hdn_tok = self.getLeftHiddenToken(child)
                        self.cnct_spaces.append(hdn_tok)
                else:
                    self.cnct_tokens.append(tok_txt)
                    hdn_tok = self.getLeftHiddenToken(child)
                    self.cnct_spaces.append(hdn_tok)

            #check if expr is function or cast
            elif isinstance(child, TDantlrParser.ExprRule12Context) or isinstance(child, TDantlrParser.ExprRule15Context):
                self.cnct_tokens.append(self.fn_conv_txt[str(str(child.getSourceInterval()[0]))])
                hdn_tok = self.getLeftHiddenToken(child)
                self.cnct_spaces.append(hdn_tok)

            #check if expr is concat
            elif isinstance(child, TDantlrParser.ExprRule07Context):
                self.cnct_tokens.append(self.cnct_conv_txt[str(child.getSourceInterval()[0])])
                hdn_tok = self.getLeftHiddenToken(child)
                self.cnct_spaces.append(hdn_tok)

            #check if expr is plus/minus interval
            elif isinstance(child, TDantlrParser.ExprRule08Context) and isinstance(child.getChild(2).getChild(0), TDantlrParser.Interval_exprContext):
                self.cnct_tokens.append(self.intv_conv_txt[str(child.getSourceInterval()[0])])
                hdn_tok = self.getLeftHiddenToken(child)
                self.cnct_spaces.append(hdn_tok)

            else:
                self.walkConcatExprChild(child)

##case specific##

    def enterCase_specific(self, ctx:TDantlrParser.Case_specificContext):
        self.case_spf = True

    def exitCase_specific(self, ctx:TDantlrParser.Case_specificContext):
        self.case_spf = False




##qualify##
    def enterSelect_expr(self, ctx:TDantlrParser.Select_exprContext):

        #alias logic -----------------------
        #print(str(str(ctx.getSourceInterval()[0])))
        self.alias_scope += ':' + str(ctx.getSourceInterval()[0])
        self.alias_scope = self.alias_scope.strip(':')
        #print(self.alias_scope)
        self.alias_scope_txt[self.alias_scope] = {}
        #-----------------------------------   
    
        #qualify logic -----------------------
        if len(self.qlfy_scope_stat) > 0:
            self.qlfy_scope += 1

        has_qlfy = False
        for child in ctx.getChildren():
            if isinstance(child.getChild(0), TDantlrParser.Qualify_exprContext):
                has_qlfy = True
                break
        self.qlfy_scope_stat.append(has_qlfy)

    def exitSelect_expr(self, ctx:TDantlrParser.Select_exprContext):
    
        #alias logic -----------------------
        if ':' in self.alias_scope:
            self.alias_scope = self.alias_scope[:self.alias_scope.rindex(':')]
            #print(self.alias_scope)
        else:
            self.alias_scope = ''
        #-----------------------------------       
        
        #qualify logic -----------------------    
        if self.qlfy_scope_stat[self.qlfy_scope]:
            self.qlfy_sub_txt[str(self.qlfy_scope)+'.'+'S1'] = 'select ' + self.qlfy_sub_txt[str(self.qlfy_scope)+'.'+'S1'].strip(',') + ' from ( '

            for i in range(1,4):
                key = str(self.qlfy_scope) + '.S' + str(i)
                self.out_sql = self.out_sql.replace('[' + key + ']', self.qlfy_sub_txt[key])

        self.qlfy_scope -= 1

    def enterSelect_list(self, ctx:TDantlrParser.Select_listContext):
        if self.qlfy_scope_stat[self.qlfy_scope] and ctx.getChild(0).getText() == '*':
            self.qlfy_scope_stat[self.qlfy_scope] = False

            scp = str(self.qlfy_scope)
            self.out_sql = self.out_sql.replace('['+scp+'.S1'+']','').replace('['+scp+'.S2'+']','')

    def enterSelect_list_expr(self, ctx:TDantlrParser.Select_list_exprContext):
        if self.qlfy_scope_stat[self.qlfy_scope]:
            col_name = ''
            chld_cnt = ctx.getChildCount()

            if isinstance(ctx.getChild(chld_cnt-1), TDantlrParser.Expr_alias_nameContext):
                col_name = 'QS' + str(self.qlfy_scope) + '.' + ctx.getChild(chld_cnt-1).getText()
            elif isinstance(ctx.getChild(0), TDantlrParser.ExprRule02Context):
                gr_chld = ctx.getChild(0).getChild(0)
                grc_chld_cnt = gr_chld.getChildCount()
                if grc_chld_cnt == 1:
                    col_name = 'QS' + str(self.qlfy_scope) + '.' + gr_chld.getChild(0).getText()
                else:
                    col_name = 'QS' + str(self.qlfy_scope) + '.' + gr_chld.getChild(grc_chld_cnt-1).getText()
            else:
                pass

            if self.qlfy_sub_txt.get(str(self.qlfy_scope)+'.S1') is None and col_name != '':
                self.qlfy_sub_txt[str(self.qlfy_scope)+'.S1'] = col_name + ",\n"
            else:
                self.qlfy_sub_txt[str(self.qlfy_scope)+'.S1'] += col_name + ",\n"


    def enterQualify_expr(self, ctx:TDantlrParser.Qualify_exprContext):
        if self.qlfy_scope_stat[self.qlfy_scope]:
            self.qlfy_expr = True
    
            scp = str(self.qlfy_scope)
            hdn_tok = self.getLeftHiddenToken(ctx)
            
            self.out_sql += hdn_tok + ') QS' + str(self.qlfy_scope) + ' where' + '['+scp+'.S3'+']'


    def exitQualify_expr(self, ctx:TDantlrParser.Qualify_exprContext):
        if self.qlfy_scope_stat[self.qlfy_scope]:

            if self.qlfy_sub_txt.get(str(self.qlfy_scope)+'.S2') == None and self.qlfy_sub_txt.get(str(self.qlfy_scope)+'.S3') == None:
                self.qlfy_sub_txt[str(self.qlfy_scope)+'.S2'] = ''
                self.qlfy_sub_txt[str(self.qlfy_scope)+'.S3'] = ''

            search_ctx = ctx.getChild(1).getChild(0)
            if isinstance(search_ctx, TDantlrParser.ExprRule11Context):
                self.walkQualifyExpr(search_ctx)
            else:
                self.walkQlfyCondition(search_ctx)

            self.qlfy_expr = False


    def walkQualifyExpr(self, ctx):
        for child in ctx.getChildren():

            if isinstance(child, TerminalNodeImpl):
                tok_txt = child.getText()
                hdn_tok = self.getLeftHiddenToken(child)
                self.qlfy_sub_txt[str(self.qlfy_scope)+'.S3'] += hdn_tok + tok_txt

            #check if expr is function or cast
            elif isinstance(child, TDantlrParser.ExprRule12Context) or isinstance(child, TDantlrParser.ExprRule15Context):
                tok_txt = self.fn_conv_txt[str(str(child.getSourceInterval()[0]))]
                hdn_tok = self.getLeftHiddenToken(child)
                self.qlfy_sub_txt[str(self.qlfy_scope)+'.S3'] += hdn_tok + tok_txt

            #check if expr is concat
            elif isinstance(child, TDantlrParser.ExprRule07Context):
                tok_txt = self.cnct_conv_txt[str(child.getSourceInterval()[0])]
                hdn_tok = self.getLeftHiddenToken(child)
                self.qlfy_sub_txt[str(self.qlfy_scope)+'.S3'] += hdn_tok + tok_txt

            #check if expr is plus/minus interval
            elif isinstance(child, TDantlrParser.ExprRule08Context) and isinstance(child.getChild(2).getChild(0), TDantlrParser.Interval_exprContext):
                tok_txt = self.intv_conv_txt[str(child.getSourceInterval()[0])]
                hdn_tok = self.getLeftHiddenToken(child)
                self.qlfy_sub_txt[str(self.qlfy_scope)+'.S3'] += hdn_tok + tok_txt

            #check if condition expr
            elif isinstance(child, TDantlrParser.ExprRule10Context):
                self.walkQlfyCondition(child)

            else:
                self.walkQualifyExpr(child)

    def walkQlfyCondition(self, ctx):
        ctx_l_child = ctx.getChild(0)
        ctx_cond_op = ctx.getChild(1)
        ctx_r_child = ctx.getChild(2)
        
        if isinstance(ctx_l_child, TDantlrParser.ExprRule13Context):
             l_expr_anly = True
        elif isinstance(ctx_l_child, TDantlrParser.ExprRule14Context):
            ctx_l_child = ctx_l_child.getChild(1)
            if isinstance(ctx_l_child, TDantlrParser.ExprRule13Context):
                l_expr_anly = True
        else:
            l_expr_anly = False        
        
        if isinstance(ctx_r_child, TDantlrParser.ExprRule13Context):
            r_expr_anly = True
        elif isinstance(ctx_r_child, TDantlrParser.ExprRule14Context):
            ctx_r_child = ctx_r_child.getChild(1)
            if isinstance(ctx_r_child, TDantlrParser.ExprRule13Context):
                r_expr_anly = True
        else:
            r_expr_anly = False

        if l_expr_anly == True and r_expr_anly == True:
        
            l_alias = 'qc' + str(self.qlfy_al_cntr)
            self.qlfy_al_cntr += 1

            r_alias = 'qc' + str(self.qlfy_al_cntr)
            self.qlfy_al_cntr += 1          
            
            self.qlfy_str = ''
            self.walkQlfyCondChild(ctx_l_child)
            self.qlfy_sub_txt[str(self.qlfy_scope)+'.'+'S2'] += self.qlfy_str.strip() + ' as ' + l_alias + ",\n"            
            
            self.qlfy_str = ''
            self.walkQlfyCondChild(ctx_r_child)
            self.qlfy_sub_txt[str(self.qlfy_scope)+'.'+'S2'] += self.qlfy_str.strip() + ' as ' + r_alias + ",\n"
            
            q_alias = 'QS' + str(self.qlfy_scope)
            self.qlfy_sub_txt[str(self.qlfy_scope)+'.'+'S3'] += q_alias + '.' + l_alias + ctx_cond_op.getText() + ' ' + q_alias +'.' + r_alias

        elif l_expr_anly == False and r_expr_anly == False:
            self.qlfy_str = ''
            self.walkQlfyCondChild(ctx_l_child, 'N')
            l_qlfy_str = self.qlfy_str
            
            self.qlfy_str = ''
            self.walkQlfyCondChild(ctx_r_child, 'N')
            r_qlfy_str = self.qlfy_str 
            
            q_alias = 'QS' + str(self.qlfy_scope)
            self.qlfy_sub_txt[str(self.qlfy_scope)+'.'+'S3'] += l_qlfy_str + ctx_cond_op.getText() + r_qlfy_str
            
        else:
            c_alias = 'qc' + str(self.qlfy_al_cntr)
            self.qlfy_al_cntr += 1            
           
            if l_expr_anly == False and r_expr_anly == True:
                temp = ctx_l_child
                ctx_l_child = ctx_r_child
                ctx_r_child = temp
                                
            self.qlfy_str = ''
            self.walkQlfyCondChild(ctx_l_child)
            self.qlfy_sub_txt[str(self.qlfy_scope)+'.'+'S2'] += self.qlfy_str.strip() + ' as ' + c_alias + ",\n"
            
            self.qlfy_str = ''
            self.walkQlfyCondChild(ctx_r_child)
            q_alias = 'QS' + str(self.qlfy_scope)
            self.qlfy_sub_txt[str(self.qlfy_scope)+'.'+'S3'] += ' ' + q_alias + '.' + c_alias + ' ' + ctx_cond_op.getText() + ' ' + self.qlfy_str.strip()
        
        
    def walkQlfyCondChild(self, ctx, flag = 'Y'):
        for child in ctx.getChildren():
            
            if isinstance(child,TerminalNodeImpl):            
                    tok_txt = child.getText()
                    hdn_tok = self.getLeftHiddenToken(child)            
                    
                    if flag == 'Y':
                        if isinstance(child.parentCtx, TDantlrParser.Column_nameContext) and not(self.alias_order_by) and tok_txt in self.alias_scope_txt[self.alias_scope].keys():
                            tok_txt = self.alias_scope_txt[self.alias_scope][tok_txt]
                    
                    self.qlfy_str += hdn_tok + tok_txt    
        
            #check if expr is function or cast   
            elif isinstance(child, TDantlrParser.ExprRule12Context) or isinstance(child, TDantlrParser.ExprRule15Context):
                tok_txt = self.fn_conv_txt[str(str(child.getSourceInterval()[0]))]
                hdn_tok = self.getLeftHiddenToken(child)
                self.qlfy_str += hdn_tok + tok_txt
                
            #check if expr is concat
            elif isinstance(child, TDantlrParser.ExprRule07Context):
                tok_txt = self.cnct_conv_txt[str(child.getSourceInterval()[0])]
                hdn_tok = self.getLeftHiddenToken(child)
                self.qlfy_str += hdn_tok + tok_txt
            
            #check if expr is plus/minus interval
            elif isinstance(child, TDantlrParser.ExprRule08Context) and isinstance(child.getChild(2).getChild(0), TDantlrParser.Interval_exprContext):
                tok_txt = self.intv_conv_txt[str(child.getSourceInterval()[0])]
                hdn_tok = self.getLeftHiddenToken(child)
                self.qlfy_str += hdn_tok + tok_txt
            
            else:
                self.walkQlfyCondChild(child, flag)     






##common##

    def getLeftHiddenToken(self,ctx):
        tok_pos = ctx.getSourceInterval()[0]
        hdn_tok = ''
        while tok_pos > 0 and self.token_stream[tok_pos - 1].channel == 1 and self.token_stream[tok_pos - 1].text in(" ","\t","\n"):
            hdn_tok = self.token_stream[tok_pos - 1].text + hdn_tok
            tok_pos -= 1
        return hdn_tok

    def visitTerminal(self, ctx:TerminalNodeImpl):
        if self.fn_nest_cnt == 0 and \
           self.intv_nest_cnt == 0 and \
           self.cnct_nest_cnt == 0 and \
           not(self.fld_mode_cast) and \
           not(self.case_spf) and \
           not(self.qlfy_expr):

            tok_txt = ctx.getText()
            hdn_tok = self.getLeftHiddenToken(ctx)

            if tok_txt.upper() == 'SELECT' and self.qlfy_scope_stat[self.qlfy_scope]:
                tok_txt = '[' + str(self.qlfy_scope) + '.S1]' + hdn_tok + tok_txt + ' [' + str(self.qlfy_scope) + '.S2]'
                self.out_sql += hdn_tok + tok_txt

            elif isinstance(ctx.parentCtx, TDantlrParser.Column_nameContext) and not(self.alias_order_by) and tok_txt in self.alias_scope_txt[self.alias_scope].keys():
                self.out_sql += hdn_tok + self.alias_scope_txt[self.alias_scope][tok_txt]
            else:
                if tok_txt == '<EOF>':
                    self.out_sql += hdn_tok + ';'
                else:
                    self.out_sql += hdn_tok + tok_txt


