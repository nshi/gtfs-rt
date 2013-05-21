google.maps.visualRefresh = true;

var map;
// Start position
var startMarker;
// End position
var endMarker;
// Realtime bus locations
var busPositions = {};
// true to show bus locations
var showPositions = false;
// Google Maps direction service
var dirServ = new google.maps.DirectionsService();

function initialize() {
  var mapOptions = {
    zoom: 13,
    center: new google.maps.LatLng(42.373637,-71.109352), // Cambridge
    mapTypeId: google.maps.MapTypeId.ROADMAP,
    streetViewControl: false
  };
  map = new google.maps.Map(document.getElementById('map-canvas'),
      mapOptions);

  setupMarkers();

  // Create the DIV to hold the position toggle button
  placePositionBtn(map);

  updatePositions([{'name': '34', 'lat': 42.573563, 'lng': -71.221619}]);

  google.maps.event.addListener(map, 'click', function(event) {
    placeStartMarker(event.latLng);
  });
}

function setupMarkers() {
  startMarker = new google.maps.Marker({
      position: new google.maps.LatLng(42.374588,-71.117291), // Harvard
      map: map,
      title: 'Start from here',
      animation: google.maps.Animation.DROP,
      draggable: true
  });
  google.maps.event.addListener(startMarker, 'dragend', function() {
    console.log('requesting transit by moving the start marker');
    requestTransit(startMarker.getPosition(), endMarker.getPosition());
  });

  endMarker = new google.maps.Marker({
      position: new google.maps.LatLng(42.359686,-71.094289), // MIT
      map: map,
      title: 'End here',
      animation: google.maps.Animation.DROP,
      draggable: true
  });
  google.maps.event.addListener(endMarker, 'dragend', function() {
    console.log('requesting transit by moving the end marker');
    requestTransit(startMarker.getPosition(), endMarker.getPosition());
  });
}

function placeStartMarker(location) {
  startMarker.setPosition(location);

  console.log('requesting transit');
  requestTransit(location, endMarker.getPosition());

  map.panTo(location);
}

function updatePositions(positions) {
  positions.forEach(function(element) {
    var marker = busPositions[element['name']];
    if (!marker) {
      marker = createPositionMarker(element);
      busPositions[element['name']] = marker;
    }

    marker.setPosition(new google.maps.LatLng(element['lat'], element['lng']));
  });
}

function createPositionMarker(position) {
  return new google.maps.Marker({
      position: new google.maps.LatLng(position['lat'], position['lng']),
      map: showPositions ? map : null,
      title: position['name'],
      icon: 'https://google-maps-icons.googlecode.com/files/bus.png'
  });
}

function togglePositionMarkerVisibility(visible) {
  for (var key in busPositions) {
    if (visible) {
      busPositions[key].setMap(map);
    } else {
      busPositions[key].setMap(null);
    }
  }
}

function requestTransit(startLocation, endLocation) {
  var dirReq = {
    origin: startLocation,
    destination: endLocation,
    provideRouteAlternatives: true,
    travelMode: google.maps.TravelMode.TRANSIT
  };

  dirServ.route(dirReq, function(result, status) {
    if (status === 'OK') {
      var busRoutes = findBusRoute(result);
      populateRoutesTable(busRoutes);
    } else {
      printMsg('No valid results: ' + status, true);
      console.warn(result);
    }
  });
}

function findBusRoute(directions) {
  var busRoutes = {};

  for (var routeIndex = 0;
       routeIndex < directions['routes'].length;
       routeIndex++) {
    var busStep = findFirstBusStep(directions['routes'][routeIndex]);
    if (busStep) {
      var stopInfo = extractStopInfo(busStep);
      var busInfo = extractBusInfo(busStep);

      busRoutes[busInfo] = stopInfo;
    } else {
      console.log('No eligible bus routes');
      console.log(directions['routes'][routeIndex]);
    }
  }

  console.log(busRoutes);

  return busRoutes;
}

// Returns a bus step or null
function findFirstBusStep(route) {
  // only care about the first leg
  var steps = route['legs'][0]['steps'];

  // only care about the first non-walking step
  var firstBusStep;
  for (var stepIndex = 0; stepIndex < steps.length; stepIndex++) {
    if (steps[stepIndex]['travel_mode'] === 'WALKING') {
      continue;
    }

    if (steps[stepIndex]['travel_mode'] === 'TRANSIT' &&
        steps[stepIndex]['transit']['line']['vehicle']['type'] === 'BUS' &&
        steps[stepIndex]['transit']['line']['agencies'][0]['name'] === 'MBTA') {
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
    return step['transit']['departure_stop'];
  } else {
    return null;
  }
}

function extractBusInfo(step) {
  if (step) {
    return step['transit']['line']['short_name'];
  } else {
    return null;
  }
}

function placePositionBtn(map) {
  var positionControlDiv = document.createElement('div');
  var positionControl = new createPositionBtn(positionControlDiv, map);
  positionControlDiv.index = 1;
  map.controls[google.maps.ControlPosition.TOP_RIGHT].push(positionControlDiv);
}

function createPositionBtn(controlDiv, map) {
  // Set CSS styles for the DIV containing the control
  // Setting padding to 5 px will offset the control
  // from the edge of the map.
  controlDiv.style.padding = '5px';

  // Set CSS for the control border.
  var controlUI = document.createElement('div');
  controlUI.className = 'btn btn-small';
  // Set default text
  togglePositionBtn(controlUI);
  controlDiv.appendChild(controlUI);

  // Setup the click event listeners: simply set the map to Chicago.
  google.maps.event.addDomListener(controlUI, 'click', function() {
    showPositions = togglePositionBtn(controlUI);
    togglePositionMarkerVisibility(showPositions);
  });
}

/**
 * Returns true to show positions, false to hide them.
 */
function togglePositionBtn(btn) {
  if (btn.innerHTML === 'Show Positions') {
    btn.innerHTML = 'Hide Positions';
    btn.title = 'Hide real-time bus positions';
    return true;
  } else {
    btn.innerHTML = 'Show Positions';
    btn.title = 'Show real-time bus positions';
    return false;
  }
}

function query() {
  var flickerAPI = "http://api.flickr.com/services/feeds/photos_public.gne?jsoncallback=?";
  $.getJSON(flickerAPI, {
    tags: "mount rainier",
    tagmode: "any",
    format: "json"
  }).done(function(data) {
    $.each(data.items, function(i, item) {
      console.log(item);
      $('<img/>').attr('src', item.media.m).appendTo('#images');
      if ( i === 3 ) {
        return false;
      } else {
        return true;
      }
    });
  });
}

function clearRouteTable() {
  $('#routes > tbody:last').empty();
}

function addRouteToTable(route) {
  var subscribe = '<form class="form-inline">' +
        '<input type="hidden" name="route" value="' + route + '">' +
        '<input class="input-small" id="subscribeButton" name="delay" type="number" placeholder="minutes">' +
        '</form>';
  var row = '<tr><td>' + route + '</td><td>' + subscribe + '</td></tr>';
  $('#routes > tbody:last').append(row);

  // Disable submit on Enter
  $('form').bind('keyup', function(e) {
    var code = e.keyCode || e.which;
    if (code  == 13) {
      e.preventDefault();
      return false;
    } else {
      return true;
    }
  });

  // Use Ajax to submit the form so the page doesn't refresh
  $('#subscribeButton').keydown(function() {
    if (event.keyCode == 13) {
      console.log(this.form);
      return false;
    } else {
      return true;
    }
  });
}

function populateRoutesTable(busRoutes) {
  clearRouteTable();
  for (var route in busRoutes) {
    addRouteToTable(route);
  }
}

function printMsg(msg, warn) {
  var msgContainer = $('#msg-pane');
  msgContainer.text(msg);

  if (warn) {
    msgContainer.addClass('text-error');
  } else {
    msgContainer.removeClass('text-error');
  }
}

google.maps.event.addDomListener(window, 'load', initialize);
