package commons

import "time"

type EventKind string

const (
	CartCreated      EventKind = "CART_CREATED"
	CartUpdated      EventKind = "CART_UPDATED"
	CartCleared      EventKind = "CART_CLEARED"
	CartDeleted      EventKind = "CART_DELETED"
	CheckoutStarted  EventKind = "CHECKOUT_STARTED"
	PurchaseComplete EventKind = "PURCHASE_COMPLETED"
	OwnerChanged     EventKind = "CART_OWNER_CHANGED"
)

type Item struct {
	ProductID string `json:"product_id"`
	Name      string `json:"name"`
	Quantity  int    `json:"quantity"`
}

// Recipient contains only the identity and contact fields used by recovery.
type Recipient struct {
	UserID         string `json:"user_id,omitempty"`
	GuestID        string `json:"guest_id,omitempty"`
	FirstName      string `json:"first_name,omitempty"`
	ContactRef     string `json:"contact_ref,omitempty"`
	ContactAllowed bool   `json:"contact_allowed"`
}

type CartContents struct {
	Items []Item `json:"items"`
	URL   string `json:"url"`
}

// Snapshot is the recovery projection supplied by the existing cart service.
// Keeping recipient and cart fields together avoids lookups while rendering;
// neither field is a full customer profile or the authoritative cart record.
type Snapshot struct {
	Recipient Recipient    `json:"recipient"`
	Cart      CartContents `json:"cart"`
}

type Event struct {
	ID         string
	CartID     string
	Revision   int64
	Kind       EventKind
	OccurredAt time.Time
	LastEditAt time.Time // Used to bootstrap an ownership event's full snapshot.
	Snapshot   Snapshot
}

type CartStatus string

const (
	Active    CartStatus = "ACTIVE"
	Abandoned CartStatus = "ABANDONED"
	Resumed   CartStatus = "RESUMED"
	Completed CartStatus = "COMPLETED"
)

type Policy struct {
	Version     string
	Inactivity  time.Duration
	Offsets     []time.Duration
	MaxLateness time.Duration
}

func DefaultPolicy() Policy {
	return Policy{
		Version: "v1", Inactivity: 30 * time.Minute,
		Offsets:     []time.Duration{30 * time.Minute, time.Hour, 24 * time.Hour},
		MaxLateness: 15 * time.Minute,
	}
}

type Cart struct {
	ID        string
	Revision  int64
	Status    CartStatus
	LastEdit  time.Time
	AbandonAt time.Time
	Snapshot  Snapshot
	Policy    Policy
}

type ReminderStatus string

const (
	Pending    ReminderStatus = "PENDING"
	Processing ReminderStatus = "PROCESSING"
	Cancelled  ReminderStatus = "CANCELLED"
	Triggered  ReminderStatus = "TRIGGERED"
	Failed     ReminderStatus = "FAILED"
	Expired    ReminderStatus = "EXPIRED"
)

type Message struct {
	Subject string
	Body    string
}

type Reminder struct {
	ID            string
	CartID        string
	Step          int
	DueAt         time.Time
	ExpiresAt     time.Time
	Status        ReminderStatus
	ConfigVersion string
	RetryCount    int // Expired-lease reclaims; the first claim is not a retry.
	LeaseUntil    time.Time
	LastError     string
	Message       Message
	TriggeredAt   time.Time
}

// Claim includes the snapshot used for rendering and the exact lease fence.
type Claim struct {
	Reminder Reminder
	Snapshot Snapshot
}
