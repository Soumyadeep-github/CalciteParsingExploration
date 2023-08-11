package succeeded


object CaseTests extends App{

  val b = CaseClassTesting(1)
  val c : Seq[CaseClassTesting] = Seq(b)
//  c.map(_.id).distinct
  val x = 2L
  type g = Long
  x match {
  case _ : Long => println("@")

  case _: Any =>
  }

}
