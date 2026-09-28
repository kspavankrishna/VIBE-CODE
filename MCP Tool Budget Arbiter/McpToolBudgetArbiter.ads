with Ada.Real_Time;
with Ada.Strings.Bounded;
with Interfaces;

--  MCP_Tool_Budget_Arbiter gives a fleet of concurrent AI agent tasks a
--  single shared spend budget that none of them can ever push negative and
--  that survives a task crashing mid tool call. It is built around one
--  Ada idiom that most agent runtimes do not have available to them: a
--  protected object, which gives mutual exclusion and, under the
--  Ceiling_Locking policy, immunity to priority inversion, for free from
--  the language itself instead of from a hand rolled mutex.
package MCP_Tool_Budget_Arbiter is

   --  A cost unit is whatever the caller wants it to mean: micro dollars,
   --  prompt tokens, GPU milliseconds. Budget_Units is a range type that
   --  starts at zero, so any arithmetic mistake that would drive the
   --  balance below zero raises Constraint_Error at the exact statement
   --  that caused it instead of silently wrapping or drifting negative.
   type Budget_Units is range 0 .. 2 ** 62;

   Max_Tenant_Length : constant := 64;

   package Tenant_Strings is new
     Ada.Strings.Bounded.Generic_Bounded_Length (Max_Tenant_Length);

   subtype Tenant_Id is Tenant_Strings.Bounded_String;

   function To_Tenant_Id (Name : String) return Tenant_Id is
     (Tenant_Strings.To_Bounded_String (Name));

   --  FNV-1a 64 bit offset basis. Seeds every arbiter's tamper evident
   --  hash chain. Declared here, ahead of the protected type, so both the
   --  type's own default initialisation and the package body's chain
   --  verifier can see the same value.
   Initial_Chain_Seed : constant Interfaces.Unsigned_64
     := 16#CBF2_9CE4_8422_2325#;

   Max_Outstanding_Reservations : constant := 256;

   subtype Reservation_Id is Natural range 0 .. Max_Outstanding_Reservations;
   No_Reservation : constant Reservation_Id := 0;

   type Arbiter_Status is
     (Granted,
      Rejected_Invalid_Amount,
      Rejected_Insufficient_Budget,
      Rejected_Table_Full);

   type Reservation_Result is record
      Status : Arbiter_Status := Rejected_Invalid_Amount;
      Id     : Reservation_Id := No_Reservation;
   end record;

   Max_Ledger_Entries : constant := 1024;

   type Ledger_Event_Kind is (Reserved, Committed, Rolled_Back, Expired);

   type Ledger_Record is record
      Sequence : Natural;
      Event    : Ledger_Event_Kind;
      Tenant   : Tenant_Id;
      Amount   : Budget_Units;
      Chain    : Interfaces.Unsigned_64;
   end record;

   type Ledger_Window is array (Positive range <>) of Ledger_Record;

   --  One Arbiter guards one shared budget. Capacity is the total budget
   --  handed out over the object's lifetime that is currently available
   --  to reserve; Lease_Milliseconds bounds how long a granted
   --  reservation may sit uncommitted before Expire_Stale is allowed to
   --  reclaim it, so a task that reserves and then crashes or hangs
   --  cannot starve the budget forever.
   --
   --  No operation here allocates from the heap: the reservation table
   --  and the ledger are both fixed size arrays sized at compile time, so
   --  this type is usable on a restricted, no-heap Ada runtime exactly as
   --  it is on a native one.
   --
   --  Reservation_Slot, Slot_Table and Ledger_Table exist only so the
   --  Arbiter below has somewhere to put its state. A protected type's own
   --  private section may declare components of an existing type but may
   --  not define a brand new record type in place, so these three have to
   --  live out here instead. Nothing outside this package can read or
   --  write a Slot_Table or a Ledger_Table regardless, because the only
   --  objects of these types are the private components of an Arbiter,
   --  reachable solely through its own operations.
   type Reservation_Slot is record
      In_Use     : Boolean := False;
      Tenant     : Tenant_Id;
      Amount     : Budget_Units := 0;
      Issued_At  : Ada.Real_Time.Time;
      Expires_At : Ada.Real_Time.Time;
   end record;

   type Slot_Table is
     array (1 .. Max_Outstanding_Reservations) of Reservation_Slot;

   type Ledger_Table is
     array (0 .. Max_Ledger_Entries - 1) of Ledger_Record;

   protected type Arbiter
     (Capacity           : Budget_Units;
      Lease_Milliseconds : Positive)
   is

      --  Reserve never blocks. It either grants a ticket immediately or
      --  rejects the call immediately, so a caller under load gets a fast
      --  answer and decides its own retry or backoff policy instead of
      --  queueing invisibly inside the arbiter. Amount = 0 always comes
      --  back Rejected_Invalid_Amount and nothing else does; this is
      --  enforced by the body rather than by a Post aspect here, because
      --  GNAT 13's code generator for postconditions on a discriminated
      --  protected type crashes when the Post reads a component of an
      --  out mode record parameter (reproduced independently of this
      --  package; see the README).
      procedure Reserve
        (Tenant  : Tenant_Id;
         Amount  : Budget_Units;
         Outcome : out Reservation_Result);

      --  Settles a reservation at its real, measured cost. If the real
      --  cost is lower than what was held, the difference is refunded to
      --  the shared balance immediately. If it is higher, the charge is
      --  capped at the amount that was actually reserved, protecting the
      --  shared budget from a caller that under-quoted itself; reconciling
      --  that overrun is left to the caller's own accounting.
      procedure Commit
        (Id          : Reservation_Id;
         Actual_Cost : Budget_Units;
         Ok          : out Boolean);

      --  Returns a reservation's full amount to the shared balance
      --  without charging anything, for a tool call that was granted a
      --  slot but never actually ran.
      procedure Rollback
        (Id : Reservation_Id;
         Ok : out Boolean);

      --  Sweeps the reservation table and reclaims every ticket whose
      --  lease has expired without a Commit or Rollback: the fix for a
      --  task that reserved budget and then crashed, hung or was killed.
      procedure Expire_Stale
        (Now           : Ada.Real_Time.Time;
         Expired_Count : out Natural);

      --  Available never exceeds Capacity; this is a Post candidate in
      --  principle, but GNAT 13's postcondition code generator for a
      --  discriminated protected type also crashes when the check reads
      --  a discriminant (reproduced independently; see the README), so
      --  the guarantee is left to the body's arithmetic instead.
      function Available return Budget_Units;

      function Outstanding_Count return Natural;

      --  Recomputes the hash chain over every ledger record still held in
      --  the ring buffer and confirms it matches what was recorded at
      --  write time. This only proves the retained window is intact; see
      --  the README for exactly what that does and does not guarantee.
      function Verify_Ledger_Integrity return Boolean;

      --  Copies the ledger, oldest entry first, into Into (1 .. Count).
      --  Into must be at least Max_Ledger_Entries long so the full
      --  retained window always fits.
      procedure Copy_Ledger
        (Into  : out Ledger_Window;
         Count : out Natural)
      with Pre => Into'Length >= Max_Ledger_Entries;

   private

      Balance       : Budget_Units := Capacity;
      Slots         : Slot_Table;
      Ledger        : Ledger_Table;
      Ledger_Count  : Natural := 0;
      Next_Sequence : Natural := 0;
      Chain_Hash    : Interfaces.Unsigned_64 := Initial_Chain_Seed;
      Lease         : Ada.Real_Time.Time_Span :=
                        Ada.Real_Time.Milliseconds (Lease_Milliseconds);

   end Arbiter;

end MCP_Tool_Budget_Arbiter;
