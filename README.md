This ZipScheduler is built to handle scheduling of Zipline deliveries.

It currently operates with a relatively simple search algorithm using the 
following design principles:
    - Emergency orders are always priority, and should be sent out as quickly as 
    possible
    - 2 Zips are left on reserve for use only with Emergency orders, although 
    this number is variable
    - Resupply orders of 1 item are not scheduled unless they are 85% of the 
    maximum range or over 2 hours old (variables)
    - If more than the maximum (3) resupply orders currently exist, we prioritize 
    the oldest order, then find the next 2 remaining orders that are closest to 
    oldest order and bundle them all on 1 Zip.  Repeat for each available Zip as 
    long as resupply orders remain
    
Also constructed is a simplified test scenario that starts a loop at the first 
order time (25634), and then calls the order queue repeatedly at increments of 60 
seconds to find new orders listed within that time span.

Because we have no real-time measurement of when Zips return from their flights, 
also calculated is an estimated 'end time' for each flight, based on the distance, 
start time, and speed of the Zip.