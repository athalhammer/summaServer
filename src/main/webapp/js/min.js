$( document ).ready(function() {
	$( ".predicate" ).hover(function() {
		$( this ).parent().attr("class", "bgcolor");
		}, function() {
		$( this ).parent().removeClass();
	});
	$( ".resource" ).hover(function() {
		$( this ).attr("class", "bgcolor");
		}, function() {
		$( this ).removeClass();
	});
});