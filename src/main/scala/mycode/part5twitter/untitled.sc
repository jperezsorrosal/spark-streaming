import java.text.SimpleDateFormat
import java.util.{Date, Locale}

val pattern = "dd/MMM/yyyy:HH:mm:ss ZZZZ"

val dateformat  =new SimpleDateFormat(pattern, new Locale("en", "US"));
val  date = dateformat.format(new Date());
System.out.println(date);

val str = "29/Nov/2015:06:34:21 +0000"

val newdate = dateformat.parse(str)
println(dateformat.format(newdate))



