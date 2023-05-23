import oracledb

class OracleOperationsManager:

    def oracle_select(self, logger, db_conn, select_sql, query_data=(), read_batch_size=100000, desc_needed=False):
        db_cursor = db_conn.cursor()
        db_cursor.arraysize = read_batch_size
        success = True
        try:
            logger.debug(f'Executing query : ')
            logger.debug(select_sql)
            if query_data:
                logger.debug(f'Query data is : ')
                logger.debug(query_data)
            query_result = db_cursor.execute(select_sql, query_data).fetchall()
        except Exception as excp:
            success = False
            logger.error(f"Query failed : {excp}")
            logger.error("Query was : ")
            logger.error(select_sql)
            db_cursor.close()
            if desc_needed:
                return success, None, None
            else:
                return success, None
        else:
            if desc_needed:
                col_names = [row[0] for row in db_cursor.description]
                db_cursor.close()
                return success, query_result, col_names
            else:
                db_cursor.close()
                return success, query_result

    def oracle_dml_or_ddl(self, logger, db_conn, dml_sql, query_data=None, commit_size=10000, ignore_errors=True):
        db_cursor = db_conn.cursor()
        success = True
        logger.debug(f'Executing query : ')
        logger.debug(dml_sql)
        if not query_data:
            try:
                db_cursor.execute(dml_sql)
            except Exception as excp:
                success = False
                logger.error(f"Query failed because : {excp}")
                logger.error("Query was : ")
                logger.error(dml_sql)
            else:
                db_conn.commit()
        else:
            low_val = 0
            high_val = commit_size
            while True:
                batch_data = query_data[low_val:high_val]
                logger.debug(f'2 sample rows for dml : ')
                logger.debug(batch_data[:2])
                try:
                    if ignore_errors:
                        db_cursor.executemany(dml_sql, batch_data, batcherrors=True)
                    else:
                        db_cursor.executemany(dml_sql, batch_data)
                except Exception as excp:
                    success = False
                    logger.error(f"Query failed because : {excp}")
                    logger.error("Query was : ")
                    logger.error(dml_sql)
                    logger.error(f"Failure row range : {low_val}, {high_val}")
                    break
                else:
                    db_conn.commit()
                    if ignore_errors:
                        for error_obj in db_cursor.getbatcherrors():
                            logger.error(f"Row {error_obj.offset} has error : {error_obj.message}")

                    batch_data.clear()
                    if high_val >= len(query_data):
                        break
                        
                    low_val = high_val
                    high_val += commit_size

        db_cursor.close()

        return success

    def oracle_insert_with_id_returned(self, logger, db_conn, insert_sql, query_data):
        success = True
        db_cursor = db_conn.cursor()
        new_id = db_cursor.var(int)
        new_query_data = query_data + (new_id, )
        returned_id = None
        try:
            db_cursor.execute(insert_sql, new_query_data)
        except Exception as excp:
            success = False
            logger.error(f"Query failed because : {excp}")
            logger.error("Query was : ")
            logger.error(insert_sql)
        else:
            db_conn.commit()
            returned_id = new_id.getvalue()

        db_cursor.close()

        return success, returned_id



