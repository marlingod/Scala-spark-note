
  trait Feline {
    def colour: String

    def sound: String
  }

  /*case class Cat(colour: String, sound: String) extends Feline {
  val sound = "roar"
}
*/
  case class Cat(colour: String, food: String) extends Feline {
    val sound = "meow"
  }

  case class Lion(colour: String, nameSize: Int) extends Feline {
    val sound = "roar"
  }

  case class Tiger(colour: String) extends Feline {
    val sound = "roar"
  }
  case class Panther(sound: String) extends Feline {
    val colour = "black"
  }

  trait BigCat extends Feline {
    val sound = "roar"
  }
trait Shape {
  def sides : Int
  def perimeter : Double
  def area : Double
  //def color :
}
case  class Circle(radius: Double) extends  Shape {
  val sides =1
  val perimeter = 2 * math.Pi * radius
  val area = math.Pi * radius * radius
}
case class Rectangle(width: Double, len : Double) extends Shape{
  val sides =4
  val perimeter = 2 * width + 2* len
  val area = width * len
}
case class Square(len: Double) extends Shape {
  val sides =4
  val perimeter = 4 * len
  val area = len * len
}
trait TheRec extends Shape {
  val sides :Int =4
  def len: Double
  def width : Double
  val perimeter = 2 * width + 2* len
  val area = width * len
}

case class TheSqaure(size: Double) extends TheRec {
  val len = size
  val width =size
}
case class TheRectangle(width: Double, len: Double) extends TheRec

object Draw {
  def apply(shape: Shape): String = shape match {
    case Rectangle(width, len) =>
      s" A rectangle with ${width}cm and height ${len} cm"
    case Square(size) =>
      s" A Square with size ${size} cm"
    case Circle(radius) =>
      s" a Circle with radius ${radius} cm"
  }
}
Draw(Circle(10))

  val modelTrav = Traversable("Me", "you")
  modelTrav.foreach(println)

sealed trait Color {
  def red : Double
  def green : Double
  def blue : Double

  def isLIght = (red +blue + green)/3.0 > 0.5

  def isDark = !isLIght
}
final case object Red extends  Color {
  val red = 1.0
  val green = 0.0
  val blue =0.0
}

final case object Yellow extends  Color {
  val red = 1.0
  val green = 1.0
  val blue =0.0
}

final case object Pink extends  Color {
  val red   = 1.0
  val green = 0.0
  val blue  = 1.0
}

trait Shape2 {
  def sides : Int
  def perimeter : Double
  def area : Double
  def color : Color
}

import scala.util.Random
object CustomerID {
  def apply(name: String) = s"$name--${Random.nextLong}"

  def unapply(customerID : String): Option[String] = {
    val name =customerID.split("--").head
    if (name.nonEmpty)Some(name) else None
  }
}

/*val customer1ID = CustomerID("SukYong")
customer1ID match {
  case CustomerID(name) => println(name)
  case _ => print("could not find it")
}*/

sealed trait TrafficLight {
  def next: TrafficLight
}

final case object Red1 extends TrafficLight {
  def next: TrafficLight = Green1
}

final case object Green1 extends TrafficLight {
  def next: TrafficLight = Yellow1
}

final case object Yellow1 extends TrafficLight {
  def next: TrafficLight = Red1
}

sealed trait TrafficLight1 {
  def next: TrafficLight1 =
    this match {
      case Red2 => Green2
      case Green2 => Yellow2
      case Yellow2 => Red2
    }
}

final case object Red2 extends TrafficLight1
final case object Green2 extends TrafficLight1
final case object Yellow2 extends TrafficLight1


sealed trait Calculation
final case class Success(result:Int) extends Calculation
final case class Failure (reason: String  ) extends Calculation

object Calculator {
  def +(calc: Calculation, operand: Int): Calculation =
    calc match {
      case Success(result) => Success(result + operand)
      case Failure(reason) => Failure(reason)
    }
  def -(calc: Calculation, operand: Int): Calculation =
    calc match {
      case Success(result) => Success(result - operand)
      case Failure(reason) => Failure(reason)
    }
  def /(calc: Calculation, operand: Int) : Calculation =
    calc match {
      case Success(result) =>
        operand match {
          case 0 => Failure("Division by zero")
          case _ => Success(result/operand)
        }
      case Failure(reason) =>Failure(reason)
  }
}
assert(Calculator.+(Success(1),1) == Success(2))
