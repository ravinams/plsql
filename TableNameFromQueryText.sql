set serveroutput on 
declare 
 
  cursor csr_query_txt is
     select query_sql_text
     from hr.extraction_sql 
     where  ( upper(query_sql_text) like '%SELECT%' 
           OR upper(query_sql_text) like '%INSERT%'
           OR upper(query_sql_text) like '%DELETE%' 
           OR upper(query_sql_text) like '%UPDATE%'
           OR upper(query_sql_text) like '%MERGE%' );
     
 TYPE tab_query_txt_type  IS TABLE OF hr.extraction_sql.query_sql_text%TYPE ;
 c_sql_query_txts_table   tab_query_txt_type;
 c_limit PLS_INTEGER := 100;
 
 g_sql_query_txts_table   tab_query_txt_type;
 
 TYPE table_name_type IS TABLE OF VARCHAR2(2000) INDEX BY  BINARY_INTEGER;
 g_table_names    table_name_type;
 counter INTEGER;
 sqlText  hr.extraction_sql.query_sql_text%TYPE;
 
 tableSectionStartPosition integer ;
 tableSectionEndPosition integer ;
 tableNamesStr varchar2(4000);
 
 -- inner procedure starts here 
 procedure addTableName2GlobalCollection(p_table_name IN varchar) IS

l_table_name varchar2(100):=rtrim(ltrim(p_table_name)) ;
l_record_exists VARCHAR2(1) default 'N';
begin
   DBMS_OUTPUT.put_line ('Before Single Table Name ==> '||l_table_name);
   if ( instr(l_table_name,' ') > 0) then
      DBMS_OUTPUT.put_line ('Alias Exists');
      l_table_name := substr(l_table_name,1,instr(l_table_name,' ') -1) ;
   else  
    DBMS_OUTPUT.put_line ('Alias Does not exists');
   end if ;
    DBMS_OUTPUT.put_line ('After Single Table Name ==> '||l_table_name);
   
    FOR indx IN  g_table_names.FIRST..g_table_names.LAST LOOP
       IF g_table_names(indx) = l_table_name THEN
         l_record_exists:='Y' ;
       END IF;
   END LOOP;
    DBMS_OUTPUT.put_line (l_record_exists);
   if (l_record_exists = 'Y')then
     g_table_names(g_table_names.COUNT) := l_table_name ;
   end if ;
   
    
end ;



 procedure parseTableNames(p_tableNamesStr IN varchar) IS
  cursor c_table_names is
     select regexp_substr(p_tableNamesStr,'[^,]+', 1, level) tab_name from dual connect by regexp_substr(p_tableNamesStr, '[^,]+', 1, level) is not null;
    
l_table_name   VARCHAR2(1000);
 
 
l_record_exists VARCHAR2(1) default 'N';

 begin
 
   DBMS_OUTPUT.PUT_LINE('table clause str ==> '||p_tableNamesStr);
  
   
  FOR tab_name_rec IN (
       select regexp_substr(p_tableNamesStr,'[^,]+', 1, level) tab_name from dual connect by regexp_substr(p_tableNamesStr, '[^,]+', 1, level) is not null)
   LOOP
     l_table_name := rtrim(ltrim(tab_name_rec.tab_name));
     
       DBMS_OUTPUT.put_line ('Before Single Table Name ==> '||l_table_name);
   if ( instr(l_table_name,' ') > 0) then
      DBMS_OUTPUT.put_line ('Alias Exists');
      l_table_name := substr(l_table_name,1,instr(l_table_name,' ') -1) ;
   else  
    DBMS_OUTPUT.put_line ('Alias Does not exists');
   end if ;
    DBMS_OUTPUT.put_line ('After Exlude Alias Table Name ==> '||l_table_name);
   
    FOR indx IN 1..g_table_names.COUNT LOOP
       IF g_table_names(indx) = l_table_name THEN
         l_record_exists:='Y' ;
       END IF;
   END LOOP;
    DBMS_OUTPUT.put_line (l_record_exists);
   if (l_record_exists = 'N')then
     g_table_names(g_table_names.COUNT+1) := l_table_name ;
   end if ;
   END LOOP;


 end   ;
 -- inner procedure end here 


begin
 dbms_output.put_line('Start');
 OPEN csr_query_txt;
 LOOP
 FETCH csr_query_txt BULK COLLECT INTO c_sql_query_txts_table LIMIT c_limit;
 EXIT WHEN c_sql_query_txts_table.COUNT = 0;
 
  counter := c_sql_query_txts_table.FIRST;
   WHILE counter IS NOT NULL
   LOOP
      DBMS_OUTPUT.PUT_LINE('----------------------------------'); 
      sqlText:=upper(c_sql_query_txts_table(counter)) ;
      -- SELECT and DELETE stmts follow same query syntax
      IF ( ( instr(sqlText,'SELECT') > 0 ) OR ((instr( sqlText, 'DELETE') >0 and  instr( sqlText, 'MERGE') = 0)) ) THEN
      
       DBMS_OUTPUT.PUT_LINE(' SQL text' || counter || ' = ' || c_sql_query_txts_table(counter));
         tableSectionStartPosition := instr(sqlText,'FROM')+4 ;
         tableSectionEndPosition := instr(sqlText,'WHERE')-1;
         
         if (tableSectionEndPosition > 0) then
          tableNamesStr :=substr(sqlText,tableSectionStartPosition,( tableSectionEndPosition-tableSectionStartPosition)) ;
         else
            tableNamesStr :=substr(sqlText,tableSectionStartPosition) ;
         end if ;
         
          parseTableNames(tableNamesStr);
            
    --  DBMS_OUTPUT.PUT_LINE('fromLocation= '||tableSectionStartPosition||'; whereLocation :='||tableSectionEndPosition||';tableNamesStr:='||tableNamesStr); 
      
      ELSIF (instr( sqlText, 'INSERT') >0 and  instr( sqlText, 'MERGE') = 0) THEN
      
        -- DBMS_OUTPUT.PUT_LINE(' SQL text' || counter || ' = ' || c_sql_query_txts_table(counter));
        tableSectionStartPosition := instr(sqlText,'INTO')+4 ;
        tableSectionEndPosition := instr(sqlText,'(')-1 ;
        tableNamesStr :=substr(sqlText,tableSectionStartPosition,( tableSectionEndPosition-tableSectionStartPosition)) ;
      --  DBMS_OUTPUT.PUT_LINE('fromLocation= '||tableSectionStartPosition||'; whereLocation :='||tableSectionEndPosition||';tableNamesStr:='||tableNamesStr); 
        
         parseTableNames(tableNamesStr);
         
      ELSIF (instr( sqlText, 'DELETE') >0 and  instr( sqlText, 'MERGE') = 0) THEN
       
       null ;
       -- see the 'SELECT' section
        
        
      ELSIF (instr( sqlText, 'UPDATE') >0 and  instr( sqlText, 'MERGE') = 0) THEN
       
        DBMS_OUTPUT.PUT_LINE(' SQL text' || counter || ' = ' || c_sql_query_txts_table(counter));
        tableSectionStartPosition := instr(sqlText,'UPDATE')+6 ;
        tableSectionEndPosition := instr(sqlText,'SET')-1 ;
        tableNamesStr :=substr(sqlText,tableSectionStartPosition,( tableSectionEndPosition-tableSectionStartPosition)) ;
      --  DBMS_OUTPUT.PUT_LINE('fromLocation= '||tableSectionStartPosition||'; whereLocation :='||tableSectionEndPosition||';tableNamesStr:='||tableNamesStr); 
        
         parseTableNames(tableNamesStr);
         
      ELSIF (instr( sqlText, 'MERGE') >0 ) THEN
      
        -- DBMS_OUTPUT.PUT_LINE(' SQL text' || counter || ' = ' || c_sql_query_txts_table(counter));
        
        tableSectionStartPosition := instr(sqlText,'INTO')+4 ;
        tableSectionEndPosition := instr(sqlText,'USING')-1 ;
        tableNamesStr :=substr(sqlText,tableSectionStartPosition,( tableSectionEndPosition-tableSectionStartPosition)) ;
        --DBMS_OUTPUT.PUT_LINE('fromLocation= '||tableSectionStartPosition||'; whereLocation :='||tableSectionEndPosition||';tableNamesStr:='||tableNamesStr); 
    
     parseTableNames(tableNamesStr);
     
        tableSectionStartPosition := instr(sqlText,'USING')+5 ;
        tableSectionEndPosition := instr(sqlText,'ON')-1 ;
        tableNamesStr :=substr(sqlText,tableSectionStartPosition,( tableSectionEndPosition-tableSectionStartPosition)) ;
       -- DBMS_OUTPUT.PUT_LINE('fromLocation= '||tableSectionStartPosition||'; whereLocation :='||tableSectionEndPosition||';tableNamesStr:='||tableNamesStr); 
        
     parseTableNames(tableNamesStr);
     
      END IF ;
      
      counter := c_sql_query_txts_table.NEXT(counter);
   END LOOP;
   
 END LOOP ;
 CLOSE csr_query_txt;
 
   FOR indx IN  1..g_table_names.COUNT LOOP
    DBMS_OUTPUT.PUT_LINE('Final Table Names ==> '||g_table_names(indx));
   END LOOP;
   
   
  
 dbms_output.put_line('end');
exception 
  when others then
   dbms_output.put_line(SQLERRM);
end ;