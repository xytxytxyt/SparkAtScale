package sparkAtScale

import java.util.UUID
import org.joda.time.DateTime

case class Email(
                  msg_id: String,
                  tenant_id: UUID,
                  mailbox_id: UUID,
                  time_delivered: org.joda.time.DataTime,
                  time_forwarded: org.joda.time.DataTime,
                  time_read: org.joda.time.DataTime,
                  time_replied: org.joda.time.DataTime
                  )