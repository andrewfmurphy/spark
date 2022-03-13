/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.errors

import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.{AnalysisException, IntegratedUDFTestUtils, QueryTest}
import org.apache.spark.sql.functions.{grouping, grouping_id, session_window, sum, to_timestamp}
import org.apache.spark.sql.test.SharedSparkSession

case class StringLongClass(a: String, b: Long)

case class StringIntClass(a: String, b: Int)

case class ComplexClass(a: Long, b: StringLongClass)

class QueryCompilationErrorsSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("CANNOT_UP_CAST_DATATYPE: invalid upcast data type") {
    val msg1 = intercept[AnalysisException] {
      sql("select 'value1' as a, 1L as b").as[StringIntClass]
    }.message
    assert(msg1 ===
      s"""
         |Cannot up cast b from bigint to int.
         |The type path of the target object is:
         |- field (class: "scala.Int", name: "b")
         |- root class: "org.apache.spark.sql.errors.StringIntClass"
         |You can either add an explicit cast to the input data or choose a higher precision type
       """.stripMargin.trim + " of the field in the target object")

    val msg2 = intercept[AnalysisException] {
      sql("select 1L as a," +
        " named_struct('a', 'value1', 'b', cast(1.0 as decimal(38,18))) as b")
        .as[ComplexClass]
    }.message
    assert(msg2 ===
      s"""
         |Cannot up cast b.`b` from decimal(38,18) to bigint.
         |The type path of the target object is:
         |- field (class: "scala.Long", name: "b")
         |- field (class: "org.apache.spark.sql.errors.StringLongClass", name: "b")
         |- root class: "org.apache.spark.sql.errors.ComplexClass"
         |You can either add an explicit cast to the input data or choose a higher precision type
       """.stripMargin.trim + " of the field in the target object")
  }

  test("UNSUPPORTED_GROUPING_EXPRESSION: filter with grouping/grouping_Id expression") {
    val df = Seq(
      (536361, "85123A", 2, 17850),
      (536362, "85123B", 4, 17850),
      (536363, "86123A", 6, 17851)
    ).toDF("InvoiceNo", "StockCode", "Quantity", "CustomerID")
    Seq("grouping", "grouping_id").foreach { grouping =>
      val errMsg = intercept[AnalysisException] {
        df.groupBy("CustomerId").agg(Map("Quantity" -> "max"))
          .filter(s"$grouping(CustomerId)=17850")
      }
      assert(errMsg.message ===
        "grouping()/grouping_id() can only be used with GroupingSets/Cube/Rollup")
      assert(errMsg.errorClass === Some("UNSUPPORTED_GROUPING_EXPRESSION"))
    }
  }

  test("UNSUPPORTED_GROUPING_EXPRESSION: Sort with grouping/grouping_Id expression") {
    val df = Seq(
      (536361, "85123A", 2, 17850),
      (536362, "85123B", 4, 17850),
      (536363, "86123A", 6, 17851)
    ).toDF("InvoiceNo", "StockCode", "Quantity", "CustomerID")
    Seq(grouping("CustomerId"), grouping_id("CustomerId")).foreach { grouping =>
      val errMsg = intercept[AnalysisException] {
        df.groupBy("CustomerId").agg(Map("Quantity" -> "max")).
          sort(grouping)
      }
      assert(errMsg.errorClass === Some("UNSUPPORTED_GROUPING_EXPRESSION"))
      assert(errMsg.message ===
        "grouping()/grouping_id() can only be used with GroupingSets/Cube/Rollup")
    }
  }

  test("ILLEGAL_SUBSTRING: the argument_index of string format is invalid") {
    val e = intercept[AnalysisException] {
      sql("select format_string('%0$s', 'Hello')")
    }
    assert(e.errorClass === Some("ILLEGAL_SUBSTRING"))
    assert(e.message ===
      "The argument_index of string format cannot contain position 0$.")
  }

  test("CANNOT_USE_MIXTURE: Using aggregate function with grouped aggregate pandas UDF") {
    import IntegratedUDFTestUtils._

    val df = Seq(
      (536361, "85123A", 2, 17850),
      (536362, "85123B", 4, 17850),
      (536363, "86123A", 6, 17851)
    ).toDF("InvoiceNo", "StockCode", "Quantity", "CustomerID")
    val e = intercept[AnalysisException] {
      val pandasTestUDF = TestGroupedAggPandasUDF(name = "pandas_udf")
      df.groupBy("CustomerId")
        .agg(pandasTestUDF(df("Quantity")), sum(df("Quantity"))).collect()
    }

    assert(e.errorClass === Some("CANNOT_USE_MIXTURE"))
    assert(e.message ===
      "Cannot use a mixture of aggregate function and group aggregate pandas UDF")
  }

  test("UNSUPPORTED_FEATURE: Using Python UDF with unsupported join condition") {
    import IntegratedUDFTestUtils._

    val df1 = Seq(
      (536361, "85123A", 2, 17850),
      (536362, "85123B", 4, 17850),
      (536363, "86123A", 6, 17851)
    ).toDF("InvoiceNo", "StockCode", "Quantity", "CustomerID")
    val df2 = Seq(
      ("Bob", 17850),
      ("Alice", 17850),
      ("Tom", 17851)
    ).toDF("CustomerName", "CustomerID")

    val e = intercept[AnalysisException] {
      val pythonTestUDF = TestPythonUDF(name = "python_udf")
      df1.join(
        df2, pythonTestUDF(df1("CustomerID") === df2("CustomerID")), "leftouter").collect()
    }

    assert(e.errorClass === Some("UNSUPPORTED_FEATURE"))
    assert(e.getSqlState === "0A000")
    assert(e.message ===
      "The feature is not supported: " +
      "Using PythonUDF in join condition of join type LeftOuter is not supported")
  }

  test("UNSUPPORTED_FEATURE: Using pandas UDF aggregate expression with pivot") {
    import IntegratedUDFTestUtils._

    val df = Seq(
      (536361, "85123A", 2, 17850),
      (536362, "85123B", 4, 17850),
      (536363, "86123A", 6, 17851)
    ).toDF("InvoiceNo", "StockCode", "Quantity", "CustomerID")

    val e = intercept[AnalysisException] {
      val pandasTestUDF = TestGroupedAggPandasUDF(name = "pandas_udf")
      df.groupBy(df("CustomerID")).pivot(df("CustomerID")).agg(pandasTestUDF(df("Quantity")))
    }

    assert(e.errorClass === Some("UNSUPPORTED_FEATURE"))
    assert(e.getSqlState === "0A000")
    assert(e.message ===
      "The feature is not supported: " +
      "Pandas UDF aggregate expressions don't support pivot.")
  }

  test("REFERENCE_NOT_DEFINED: window specification is not defined in the window clause.") {
    withTempView("df") {
      val df = Seq(
        (536361, "85123A", 2, 17850),
        (536362, "85123B", 4, 17850),
        (536363, "86123A", 6, 17851)
      ).toDF("InvoiceNo", "StockCode", "Quantity", "CustomerID")
      df.createTempView("df")

      val e = intercept[AnalysisException] {
        sql("SELECT ROW_NUMBER() OVER win1 FROM df WINDOW win2 AS (PARTITION BY CustomerID)")
      }
      assert(e.errorClass === Some("REFERENCE_NOT_DEFINED"))
      assert(e.message ===
        "Reference not defined: Window specification win1 is not defined in the WINDOW clause.")
    }
  }

  test("UNSUPPORTED_FEATURE: window aggregate function with filter predicate.") {
    withTempView("df") {
      val df = Seq(
        (536361, "85123A", 2, 17850),
        (536362, "85123B", 4, 17850),
        (536363, "86123A", 6, 17851)
      ).toDF("InvoiceNo", "StockCode", "Quantity", "CustomerID")
      df.createTempView("df")

      val e = intercept[AnalysisException] {
        sql("SELECT SUM(Quantity) FILTER(WHERE Quantity > 5) " +
          "OVER (PARTITION BY CustomerID) FROM df")
      }
      assert(e.errorClass === Some("UNSUPPORTED_FEATURE"))
      assert(e.message ===
        "The feature is not supported: window aggregate function" +
          " with filter predicate is not supported yet.")
    }
  }

  test("UNSUPPORTED_FEATURE: window function inside aggregate function.") {
    withTempView("df") {
      val df = Seq(
        (536361, "85123A", 2, 17850),
        (536362, "85123B", 4, 17850),
        (536363, "86123A", 6, 17851)
      ).toDF("InvoiceNo", "StockCode", "Quantity", "CustomerID")
      df.createTempView("df")

      val e = intercept[AnalysisException] {
        sql("SELECT SUM(SUM(Quantity) OVER (PARTITION BY CustomerID)) FROM df")
      }
      assert(e.errorClass === Some("UNSUPPORTED_FEATURE"))
      assert(e.message ===
        "The feature is not supported: It is not allowed to use a window function" +
          " inside an aggregate function. Please use the inner window function in a sub-query.")
    }
  }

  test("UNSUPPORTED_FEATURE: window function not allowed.") {
    withTempView("df") {
      val df = Seq(
        (536361, "85123A", 2, 17850),
        (536362, "85123B", 4, 17850),
        (536363, "86123A", 6, 17851)
      ).toDF("InvoiceNo", "StockCode", "Quantity", "CustomerID")
      df.createTempView("df")

      val e = intercept[AnalysisException] {
        sql("SELECT CustomerID FROM df WHERE Sum(Quantity) " +
          "OVER win1 > 5 WINDOW win1 as (PARTITION BY StockCode)")
      }
      assert(e.errorClass === Some("UNSUPPORTED_FEATURE"))
      assert(e.message ===
        "The feature is not supported: It is not allowed" +
          " to use window functions inside WHERE clause.")
    }
  }

  test("WINDOW_ERROR: cannot specify window frame.") {
    withTempView("df") {
      val df = Seq(
        (536361, "85123A", 2, 17850),
        (536362, "85123B", 4, 17850),
        (536363, "86123A", 6, 17851)
      ).toDF("InvoiceNo", "StockCode", "Quantity", "CustomerID")
      df.createTempView("df")

      val e = intercept[AnalysisException] {
        sql("SELECT LAG(CustomerID) OVER " +
          "(ORDER BY Quantity ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM df")
      }
      assert(e.errorClass === Some("WINDOW_ERROR"))
      assert(e.message ===
        "Window Error: Cannot specify window frame for lag function.")
    }
  }

  test("WINDOW_ERROR: window frame did not match required frame.") {
    withTempView("df") {
      val df = Seq(
        (536361, "85123A", 2, 17850),
        (536362, "85123B", 4, 17850),
        (536363, "86123A", 6, 17851)
      ).toDF("InvoiceNo", "StockCode", "Quantity", "CustomerID")
      df.createTempView("df")

      val e = intercept[AnalysisException] {
        sql("SELECT RANK(CustomerID) OVER " +
          "(ORDER BY Quantity ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM df")
      }
      assert(e.errorClass === Some("WINDOW_ERROR"))
      assert(e.message ===
        "Window Error: Window Frame specifiedwindowframe(RowFrame, currentrow$(), currentrow$())" +
          " must match the required frame " +
          "specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$()).")
    }
  }

  test("WINDOW_ERROR: window frame not ordered.") {
    withTempView("df") {
      val df = Seq(
        (536361, "85123A", 2, 17850),
        (536362, "85123B", 4, 17850),
        (536363, "86123A", 6, 17851)
      ).toDF("InvoiceNo", "StockCode", "Quantity", "CustomerID")
      df.createTempView("df")

      val e = intercept[AnalysisException] {
        sql("SELECT RANK(CustomerID) OVER " +
          "(PARTITION BY Quantity ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM df")
      }
      assert(e.errorClass === Some("WINDOW_ERROR"))
      e.message should include("requires window to be ordered, " +
        "please add ORDER BY clause.")
    }
  }

  test("UNSUPPORTED_FEATURE: multiple time/session window expressions not supported.") {
    withTempView("df") {
      var df = Seq(
        (1, "2021-01-01 00:30:30", "2021-01-01 00:45:45", 5),
        (2, "2021-02-02 00:30:30", "2021-02-02 00:45:45", 10),
        (3, "2021-03-03 00:30:30", "2021-03-03 00:45:45", 15)
      ).toDF("id", "timestamp_in", "timestamp_out", "quantity")
      df = df.withColumn("timestamp_in", to_timestamp($"timestamp_in"))
      df = df.withColumn("timestamp_out", to_timestamp($"timestamp_out"))
      df.createTempView("df")

      val e = intercept[AnalysisException] {
        sql("SELECT SUM(quantity) FROM df GROUP by id, WINDOW(timestamp_in, '5 minutes'), " +
          "WINDOW(timestamp_out, '10 minutes')")
      }
      assert(e.errorClass === Some("UNSUPPORTED_FEATURE"))
      e.message should include("Multiple time/session window expressions would result in a " +
        "cartesian product of rows, therefore they are currently not supported.")
    }
  }

  test("WINDOW_ERROR: gap duration must be CalendarIntervalType.") {
    withTempView("df") {
      var df = Seq(
        (1, "2021-01-01 00:30:30", "2021-01-01 00:45:45", 5),
        (2, "2021-02-02 00:30:30", "2021-02-02 00:45:45", 10),
        (3, "2021-03-03 00:30:30", "2021-03-03 00:45:45", 15)
      ).toDF("id", "timestamp_in", "timestamp_out", "quantity")
      df = df.withColumn("timestamp_in", to_timestamp($"timestamp_in"))
      df = df.withColumn("timestamp_out", to_timestamp($"timestamp_out"))
      df.createTempView("df")

      val e = intercept[AnalysisException] {
        df.groupBy(session_window($"timestamp_in", $"id")).agg(sum("quantity"))
      }
      assert(e.errorClass === Some("WINDOW_ERROR"))
      assert(e.message === "Window Error: Gap duration expression used in session window " +
        "must be CalendarIntervalType, but got IntegerType")
    }
  }
}
