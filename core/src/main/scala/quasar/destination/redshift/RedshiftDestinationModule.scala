package quasar.destination.redshift

import quasar.api.destination.Destination
import quasar.connector.DestinationModule

abstract class RedshiftDestinationModule {
  def destinationType = DestinationType("redshift", 1L)
}
