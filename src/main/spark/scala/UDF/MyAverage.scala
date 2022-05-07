package scala.UDF

import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator

case class Invoice(InvoiceNo:String, quantity: Long, unitPrice: Double)
case class Average(var quantity_sum: Long, var quantity_count: Long, var unitPrice_sum: Double, var unitPrice_count: Long)

object MyAverage extends Aggregator[Invoice, Average, (Double, Double)] {
  // A zero value for this aggregation. Should satisfy the property that any b + zero = b
  def zero: Average = Average(0L, 0L, 0.0, 0L)
  // Combine two values to produce a new value. For performance, the function may modify `buffer`
  // and return it instead of constructing a new object
  def reduce(buffer: Average, invoice: Invoice): Average = {
    buffer.quantity_sum += invoice.quantity
    buffer.quantity_count += 1
    buffer.unitPrice_sum += invoice.unitPrice
    buffer.unitPrice_count += 1
    buffer
  }
  // Merge two intermediate values
  def merge(b1: Average, b2: Average): Average = {
    b1.quantity_count += b2.quantity_count
    b1.quantity_sum += b2.quantity_sum
    b1.unitPrice_sum += b2.unitPrice_sum
    b1.unitPrice_count += b2.unitPrice_count
    b1
  }
  // Transform the output of the reduction
  def finish(reduction: Average): (Double, Double) = (reduction.quantity_sum.toDouble / reduction.quantity_count
    , reduction.unitPrice_sum / reduction.unitPrice_count)
  // Specifies the Encoder for the intermediate value type
  def bufferEncoder: Encoder[Average] = Encoders.product
  // Specifies the Encoder for the final output value type
  def outputEncoder: Encoder[(Double,Double)] = Encoders.tuple(Encoders.scalaDouble,Encoders.scalaDouble)
}
