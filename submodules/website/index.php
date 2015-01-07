<!DOCTYPE html>
<html>
<head>
	<meta charset="utf-8">
    <link rel="stylesheet" href="stylesheets/default.css">
	<title>Customer Insight and Predictive analytics</title>
    <script type="text/javascript" src="http://code.jquery.com/jquery-1.4.2.min.js"></script>
    <script type="text/javascript" src="javascripts/behavior.js"></script>
    <!--[if IE]>
<script src="http://html5shim.googlecode.com/svn/trunk/html5.js"></script>
<![endif]-->
</head>
<body>
	<header id="mast">
    	<h1>Customer Insight and Predictive Analytics<br/>An Apache Spark Project</h1>
		
    </header>
    <nav id="global">
    	<ul>
        	<li><a class="selected" href="index.php">Home</a></li>
		</ul>
    </nav>
    <section id="intro">
    	<header>
    		<h2>Select Category and Opinion:</h2>
        </header>
        <p>
	 
		
			<!-- Start of FORM -->
<form method="post" action="index.php" id="form1">
<?php	
	if (empty($_POST))
		{
		?>
<select name="category" id="category">
  <option selected value="1">Phone</option>
  <option value="2">Apple</option>
  <option value="3">Samsung</option>
  <option value="4">HTC</option>
</select>
		
<select name="opinion" id="opinion">
  <option selected value="1">like</option>
  <option value="2">hate</option>
</select>

	<?php 
		}else{
		?>
	
<select name="category" id="category">
  <option <?php if($_POST["category"]=="1") echo"selected"?> value="1">Phone</option>
  <option <?php if($_POST["category"]=="2") echo"selected"?> value="2">Apple</option>
  <option <?php if($_POST["category"]=="3") echo"selected"?> value="3">Samsung</option>
  <option <?php if($_POST["category"]=="4") echo"selected"?> value="4">HTC</option>
</select>
		
<select name="opinion" id="opinion">
  <option <?php if($_POST["opinion"]=="1") echo"selected"?>  value="1">like</option>
  <option <?php if($_POST["opinion"]=="2") echo"selected"?> value="2">hate</option>
</select>
	<?php 
		}
		?>
<br />
<input type="submit" name="set" value="set">
<input type="submit" name="load" value="load" id="load">
</form>
<!-- End of FORM -->
		
		</p>
		<?php 
		
		if (!empty($_POST) && isset($_POST['load']))
		{

				
		?>
	        	<br/> <b>These are your target audience!</b>
      
		<?php 
			
			
		$handle = fopen("/home/sathishbabu3/bdproject/UserQuery/output.txt", "r");
		if ($handle) {
		    while (($line = fgets($handle)) !== false) {
			// process the line read.
			$split = explode(",", $line);
			echo "<br/> <a href='https://twitter.com/".$split[1]."'>".$split[0]."</a>";
		    }
		} else {
		    // error opening the file.
		} 
		fclose($handle);


		}else if(!empty($_POST)){
			$myfile = fopen("/home/sathishbabu3/bdproject/UserQuery/input.txt", "w") or die("Unable to open file!");
			//echo "hi";			
			$txt="";
			if( $_POST["category"] || $_POST["opinion"] )
			  {
 				//echo "Welcome ".$_POST["category"]." ".$_POST["opinion"] ;
				if($_POST["category"]=="1"){
				     if($_POST["opinion"]=="1"){					
					$txt="1.0";			
					}else{
					$txt="2.0";			
					
					}
				}else if($_POST["category"]=="2"){
					if($_POST["opinion"]=="1"){					
					$txt="3.0";			
					}else{
					$txt="4.0";			
					
					}
				}else if($_POST["category"]=="3"){
					if($_POST["opinion"]=="1"){					
					$txt="5.0";			
					}else{
					$txt="6.0";			
					
					}
				}else if($_POST["category"]=="4"){
					if($_POST["opinion"]=="1"){					
					$txt="7.0";			
					}else{
					$txt="8.0";			
					
					}
				}
			  }			

			//if($_POST['category'].equals("4")){
			  //$txt="3.0";
			  //echo "hi";
			//}


			
			fwrite($myfile, $txt);
			fclose($myfile);

			

		?>	

		        	<br/> <b>Final Answer will be shown here!</b>
       
		<?php 
		
		}else{
		
		?>

     
        	<br/> <b>Final Answer will be shown here!</b>
		    
		
        
	<?php }?>
	
    </section>

    <footer>
    	<div class="clear">
           
        </div>
    </footer>
</body>
</html>
