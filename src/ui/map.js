google.maps.visualRefresh = true;

var map;
var startMarker;
var endMarker;
var dirServ = new google.maps.DirectionsService();

function initialize() {
  var mapOptions = {
    zoom: 11,
    center: new google.maps.LatLng(42.362096,-71.063004), <!-- Boston -->
    mapTypeId: google.maps.MapTypeId.ROADMAP
  };
  map = new google.maps.Map(document.getElementById('map-canvas'),
      mapOptions);

  setupMarkers();

  google.maps.event.addListener(map, 'click', function(event) {
    placeStartMarker(event.latLng);
  });
}

function setupMarkers() {
  startMarker = new google.maps.Marker({
      position: new google.maps.LatLng(42.374588,-71.117291), <!-- Harvard -->
      map: map,
      title: "Start from here",
      animation: google.maps.Animation.DROP,
      draggable: true
  });
  google.maps.event.addListener(startMarker, 'dragend', function() {
    console.log("requesting transit by moving the start marker");
    requestTransit(startMarker.getPosition(), endMarker.getPosition());
  });

  endMarker = new google.maps.Marker({
      position: new google.maps.LatLng(42.359686,-71.094289), <!-- MIT -->
      map: map,
      title: "End here",
      animation: google.maps.Animation.DROP,
      draggable: true
  });
  google.maps.event.addListener(endMarker, 'dragend', function() {
    console.log("requesting transit by moving the end marker");
    requestTransit(startMarker.getPosition(), endMarker.getPosition());
  });
}

function placeStartMarker(location) {
  <!-- var contentString = '<div id="content">'+ -->
  <!--   '<div id="siteNotice">'+ -->
  <!--   '</div>'+ -->
  <!--   '<h2 id="firstHeading" class="firstHeading">Uluru</h2>'+ -->
  <!--   '<div id="bodyContent">'+ -->
  <!--   '<p><b>Uluru</b>, also referred to as <b>Ayers Rock</b>, is a large ' + -->
  <!--   'sandstone rock formation in the southern part of the '+ -->
  <!--   'Northern Territory, central Australia. It lies 335 km (208 mi) '+ -->
  <!--   'south west of the nearest large town, Alice Springs; 450 km '+ -->
  <!--   '(280 mi) by road. Kata Tjuta and Uluru are the two major '+ -->
  <!--   'features of the Uluru - Kata Tjuta National Park. Uluru is '+ -->
  <!--   'sacred to the Pitjantjatjara and Yankunytjatjara, the '+ -->
  <!--   'Aboriginal people of the area. It has many springs, waterholes, '+ -->
  <!--   'rock caves and ancient paintings. Uluru is listed as a World '+ -->
  <!--   'Heritage Site.</p>'+ -->
  <!--   '<p>Attribution: Uluru, <a href="http://en.wikipedia.org/w/index.php?title=Uluru&oldid=297882194">'+ -->
  <!--   'http://en.wikipedia.org/w/index.php?title=Uluru</a> (last visited June 22, 2009).</p>'+ -->
  <!--   '</div>'+ -->
  <!--   '</div>'; -->

  <!-- var infowindow = new google.maps.InfoWindow({ -->
  <!--   content: contentString -->
  <!-- }); -->

  <!-- google.maps.event.addListener(startMarker, 'click', function() { -->
  <!--   infowindow.open(map, startMarker); -->
  <!-- }); -->

  startMarker.setPosition(location);

  console.log("requesting transit");
  requestTransit(location, endMarker.getPosition());

  map.panTo(location);
}

function requestTransit(startLocation, endLocation) {
  var dirReq = {
    origin: startLocation,
    destination: endLocation,
    provideRouteAlternatives: true,
    travelMode: google.maps.TravelMode.TRANSIT
  };

  dirServ.route(dirReq, function(result, status) {
    if (status === "OK") {
      findBusRoute(result);
    }
  });
}

function findBusRoute(directions) {
  var busRoutes = {};

  for (var routeIndex = 0;
       routeIndex < directions["routes"].length;
       routeIndex++) {
    var busStep = findFirstBusStep(directions["routes"][routeIndex]);
    if (busStep) {
      var stopInfo = extractStopInfo(busStep);
      var busInfo = extractBusInfo(busStep);

      busRoutes[busInfo] = stopInfo;
    } else {
      console.log("No eligible bus routes");
      console.log(directions["routes"][routeIndex]);
    }
  }

  console.log(busRoutes);
}

// Returns a bus step or null
function findFirstBusStep(route) {
  // only care about the first leg
  var steps = route["legs"][0]["steps"];

  // only care about the first non-walking step
  var firstBusStep;
  for (var stepIndex = 0; stepIndex < steps.length; stepIndex++) {
    if (steps[stepIndex]["travel_mode"] === "WALKING") {
      continue;
    }

    if (steps[stepIndex]["travel_mode"] === "TRANSIT" &&
        steps[stepIndex]["transit"]["line"]["vehicle"]["type"] === "BUS" &&
        steps[stepIndex]["transit"]["line"]["agencies"][0]["name"] === "MBTA") {
      firstBusStep = steps[stepIndex];
      break;
    } else {
      break;
    }
  }

  return firstBusStep;
}

function extractStopInfo(step) {
  if (step) {
    return step["transit"]["departure_stop"];
  } else {
    return null;
  }
}

function extractBusInfo(step) {
  if (step) {
    return step["transit"]["line"]["short_name"];
  } else {
    return null;
  }
}

google.maps.event.addDomListener(window, 'load', initialize);
