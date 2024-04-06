#include <utility>
#include <string>

#ifndef RESERVOIR_Q10_ROWS_H
#define RESERVOIR_Q10_ROWS_H

struct MessageRow {
    unsigned long id;
    std::string location_ip;
    std::string browser_used;
    std::string content;
    unsigned long length;
    unsigned long creator_person_id;

    MessageRow(unsigned long id, std::string location_ip, std::string browser_used,
               std::string content, unsigned long length, unsigned long creator_person_id) :
               id(id),
               location_ip(std::move(location_ip)),
               browser_used(std::move(browser_used)),
               content(std::move(content)),
               length(length),
               creator_person_id(creator_person_id) {}

    bool operator==(const MessageRow &rhs) const {
        return id == rhs.id;
    }
};

struct MessageRowHash {
    std::size_t operator()(const MessageRow& row) const
    {
        using std::size_t;
        using std::hash;

        return (hash<unsigned long>()(row.id));
    }
};

struct TagRow {
    unsigned long id;
    std::string name;
    std::string url;
    unsigned long type_tag_class_id;

    TagRow(unsigned long id, std::string name, std::string url, unsigned long type_tag_class_id) :
            id(id),
            name(std::move(name)),
            url(std::move(url)),
            type_tag_class_id(type_tag_class_id) {}

    bool operator==(const TagRow &rhs) const {
        return id == rhs.id;
    }
};

struct TagRowHash {
    std::size_t operator()(const TagRow& row) const
    {
        using std::size_t;
        using std::hash;

        return (hash<unsigned long>()(row.id));
    }
};

struct TagClassRow {
    unsigned long id;
    std::string name;
    std::string url;
    unsigned long subclass_of_tag_class_id;

    TagClassRow(unsigned long id, std::string name, std::string url, unsigned long subclass_of_tag_class_id) :
            id(id),
            name(std::move(name)),
            url(std::move(url)),
            subclass_of_tag_class_id(subclass_of_tag_class_id) {}

    bool operator==(const TagClassRow &rhs) const {
        return id == rhs.id;
    }
};

struct TagClassRowHash {
    std::size_t operator()(const TagClassRow& row) const
    {
        using std::size_t;
        using std::hash;

        return (hash<unsigned long>()(row.id));
    }
};

struct PersonRow {
    unsigned long id;
    std::string first_name;
    std::string last_name;
    std::string gender;
    std::string birthday;
    std::string location_ip;
    std::string browser_used;
    unsigned long location_city_id;

    PersonRow(unsigned long id, std::string first_name, std::string last_name, std::string gender,
              std::string birthday, std::string location_ip, std::string browser_used, unsigned long location_city_id) :
                id(id),
                first_name(std::move(first_name)),
                last_name(std::move(last_name)),
                gender(std::move(gender)),
                birthday(std::move(birthday)),
                location_ip(std::move(location_ip)),
                browser_used(std::move(browser_used)),
                location_city_id(location_city_id) {}

    bool operator==(const PersonRow &rhs) const {
        return id == rhs.id;
    }
};

struct PersonRowHash {
    std::size_t operator()(const PersonRow& row) const
    {
        using std::size_t;
        using std::hash;

        return (hash<unsigned long>()(row.id));
    }
};

struct CityRow {
    unsigned long id;
    std::string name;
    std::string url;
    unsigned long part_of_place_id;

    CityRow(unsigned long id, std::string name, std::string url, unsigned long part_of_place_id) :
        id(id),
        name(std::move(name)),
        url(std::move(url)),
        part_of_place_id(part_of_place_id) {}

    bool operator==(const CityRow &rhs) const {
        return id == rhs.id;
    }
};

struct CityRowHash {
    std::size_t operator()(const CityRow& row) const
    {
        using std::size_t;
        using std::hash;

        return (hash<unsigned long>()(row.id));
    }
};

struct CountryRow {
    unsigned long id;
    std::string name;
    std::string url;
    unsigned long part_of_place_id;

    CountryRow(unsigned long id, std::string name, std::string url, unsigned long part_of_place_id) :
            id(id),
            name(std::move(name)),
            url(std::move(url)),
            part_of_place_id(part_of_place_id) {}

    bool operator==(const CountryRow &rhs) const {
        return id == rhs.id;
    }
};

struct CountryRowHash {
    std::size_t operator()(const CountryRow& row) const
    {
        using std::size_t;
        using std::hash;

        return (hash<unsigned long>()(row.id));
    }
};

struct MessageHasTagRow {
    unsigned long message_id;
    unsigned long tag_id;

    MessageHasTagRow(unsigned long message_id, unsigned long tag_id) : message_id(message_id), tag_id(tag_id) {}

    bool operator==(const MessageHasTagRow &rhs) const {
        return message_id == rhs.message_id &&
               tag_id == rhs.tag_id;
    }
};

struct MessageHasTagRowHash {
    std::size_t operator()(const MessageHasTagRow& row) const
    {
        using std::size_t;
        using std::hash;

        return (hash<unsigned long>()(row.message_id)) ^ (hash<unsigned long>()(row.tag_id));
    }
};

struct PersonKnowsPersonRow {
    unsigned long person1_id;
    unsigned long person2_id;

    PersonKnowsPersonRow(unsigned long person1_id, unsigned long person2_id):
        person1_id(person1_id),
        person2_id(person2_id) {}

    bool operator==(const PersonKnowsPersonRow &rhs) const {
        return person1_id == rhs.person1_id &&
               person2_id == rhs.person2_id;
    }
};

struct PersonKnowsPersonRowHash {
    std::size_t operator()(const PersonKnowsPersonRow& row) const
    {
        using std::size_t;
        using std::hash;

        return (hash<unsigned long>()(row.person1_id)) ^ (hash<unsigned long>()(row.person2_id));
    }
};

// RSJoin

struct Q10Row {
    MessageRow* message_row;
    MessageHasTagRow* message_has_tag_row;
    MessageHasTagRow* message_has_tag_row2;
    TagRow* tag_row;
    TagRow* tag_row2;
    TagClassRow* tag_class_row;
    PersonRow* person_row;
    PersonRow* person_row2;
    PersonKnowsPersonRow* person_knows_person_row;
    CityRow* city_row;
    CountryRow* country_row;
};

// RSJoin_opt plan1

struct MessageHasTagTagRow {
    MessageHasTagRow* message_has_tag_row;
    TagRow* tag_row;

    MessageHasTagTagRow(MessageHasTagRow *message_has_tag_row, TagRow *tag_row) :
        message_has_tag_row(message_has_tag_row),
        tag_row(tag_row) {}

    bool operator==(const MessageHasTagTagRow &rhs) const {
        return message_has_tag_row->message_id == rhs.message_has_tag_row->message_id &&
               message_has_tag_row->tag_id == rhs.message_has_tag_row->tag_id;
    }
};

struct MessageHasTagTagRowHash {
    std::size_t operator()(const MessageHasTagTagRow& row) const
    {
        using std::size_t;
        using std::hash;

        return (hash<unsigned long>()(row.message_has_tag_row->message_id)) ^ (hash<unsigned long>()(row.message_has_tag_row->tag_id));
    }
};

struct MessageHasTagTagTagClassRow {
    MessageHasTagRow* message_has_tag_row2;
    TagRow* tag_row2;
    TagClassRow* tag_class_row;

    MessageHasTagTagTagClassRow(MessageHasTagRow *message_has_tag_row2, TagRow *tag_row2, TagClassRow *tag_class_row)
            : message_has_tag_row2(message_has_tag_row2), tag_row2(tag_row2), tag_class_row(tag_class_row) {}

    bool operator==(const MessageHasTagTagTagClassRow &rhs) const {
        return message_has_tag_row2->message_id == rhs.message_has_tag_row2->message_id &&
               message_has_tag_row2->tag_id == rhs.message_has_tag_row2->tag_id;
    }
};

struct MessageHasTagTagTagClassRowHash {
    std::size_t operator()(const MessageHasTagTagTagClassRow& row) const
    {
        using std::size_t;
        using std::hash;

        return (hash<unsigned long>()(row.message_has_tag_row2->message_id)) ^ (hash<unsigned long>()(row.message_has_tag_row2->tag_id));
    }
};

struct MessagePersonCityCountryRow {
    MessageRow* message_row;
    PersonRow* person_row;
    CityRow* city_row;
    CountryRow* country_row;

    MessagePersonCityCountryRow(MessageRow *message_row, PersonRow *person_row, CityRow *city_row, CountryRow *country_row)
            : message_row(message_row), person_row(person_row), city_row(city_row), country_row(country_row) {}

    bool operator==(const MessagePersonCityCountryRow &rhs) const {
        return message_row->id == rhs.message_row->id;
    }
};

struct MessagePersonCityCountryRowHash {
    std::size_t operator()(const MessagePersonCityCountryRow& row) const
    {
        using std::size_t;
        using std::hash;

        return (hash<unsigned long>()(row.message_row->id));
    }
};

struct PersonKnowsPersonPersonRow {
    PersonKnowsPersonRow* person_knows_person_row;
    PersonRow* person_row2;

    PersonKnowsPersonPersonRow(PersonKnowsPersonRow *person_knows_person_row, PersonRow *person_row2)
            : person_knows_person_row(person_knows_person_row), person_row2(person_row2) {}

    bool operator==(const PersonKnowsPersonPersonRow &rhs) const {
        return person_knows_person_row->person1_id == rhs.person_knows_person_row->person1_id &&
               person_knows_person_row->person2_id == rhs.person_knows_person_row->person2_id;
    }
};

struct PersonKnowsPersonPersonRowHash {
    std::size_t operator()(const PersonKnowsPersonPersonRow& row) const
    {
        using std::size_t;
        using std::hash;

        return (hash<unsigned long>()(row.person_knows_person_row->person1_id)) ^ (hash<unsigned long>()(row.person_knows_person_row->person2_id));
    }
};

struct Q10FkPlan1Row {
    MessagePersonCityCountryRow* message_person_city_country_row;
    MessageHasTagTagRow* message_has_tag_tag_row;
    MessageHasTagTagTagClassRow* message_has_tag_tag_tag_class_row;
    PersonKnowsPersonPersonRow* person_knows_person_person_row;
};

// RSJoin_opt plan2

struct Q10FkPlan2LeftRow{
    PersonRow* person_row;
    PersonKnowsPersonRow* person_knows_person_row;
    PersonRow* person_row2;
    CityRow* city_row;
    CountryRow* country_row;

    Q10FkPlan2LeftRow(PersonRow *person_row, PersonKnowsPersonRow *person_knows_person_row, PersonRow *person_row2,
               CityRow *city_row, CountryRow *country_row) : person_row(person_row),
                                                           person_knows_person_row(person_knows_person_row),
                                                           person_row2(person_row2), city_row(city_row),
                                                           country_row(country_row) {}

    bool operator==(const Q10FkPlan2LeftRow &rhs) const {
        return person_knows_person_row->person1_id == rhs.person_knows_person_row->person1_id
                && person_knows_person_row->person2_id == rhs.person_knows_person_row->person2_id;
    }
};

struct Q10FkPlan2LeftRowHash {
    std::size_t operator()(const Q10FkPlan2LeftRow& row) const
    {
        using std::size_t;
        using std::hash;

        return (hash<unsigned long>()(row.person_knows_person_row->person1_id)) ^ (hash<unsigned long>()(row.person_knows_person_row->person2_id));
    }
};

struct Q10FkPlan2MiddleRow {
    MessageRow* message_row;
    MessageHasTagRow* message_has_tag_row;
    TagRow* tag_row;

    Q10FkPlan2MiddleRow(MessageRow *message_row, MessageHasTagRow *message_has_tag_row, TagRow *tag_row) : message_row(message_row),
                                                                                               message_has_tag_row(message_has_tag_row),
                                                                                               tag_row(tag_row) {}

    bool operator==(const Q10FkPlan2MiddleRow &rhs) const {
        return message_has_tag_row->message_id == rhs.message_has_tag_row->message_id
                && message_has_tag_row->tag_id == rhs.message_has_tag_row->tag_id;
    }
};

struct Q10FkPlan2MiddleRowHash {
    std::size_t operator()(const Q10FkPlan2MiddleRow& row) const
    {
        using std::size_t;
        using std::hash;

        return (hash<unsigned long>()(row.message_has_tag_row->message_id)) ^ (hash<unsigned long>()(row.message_has_tag_row->tag_id));
    }
};

struct Q10FkPlan2RightRow {
    MessageHasTagRow* message_has_tag_row2;
    TagRow* tag_row2;
    TagClassRow* tag_class_row;

    Q10FkPlan2RightRow(MessageHasTagRow *message_has_tag_row2, TagRow *tag_row2, TagClassRow *tag_class_row) :
                    message_has_tag_row2(message_has_tag_row2), tag_row2(tag_row2), tag_class_row(tag_class_row) {}

    bool operator==(const Q10FkPlan2RightRow &rhs) const {
        return message_has_tag_row2->message_id == rhs.message_has_tag_row2->message_id
                && message_has_tag_row2->tag_id == rhs.message_has_tag_row2->tag_id;
    }
};

struct Q10RightRowHash {
    std::size_t operator()(const Q10FkPlan2RightRow& row) const
    {
        using std::size_t;
        using std::hash;

        return (hash<unsigned long>()(row.message_has_tag_row2->message_id)) ^ (hash<unsigned long>()(row.message_has_tag_row2->tag_id));
    }
};

struct Q10FkPlan2Row {
    Q10FkPlan2LeftRow* left_row;
    Q10FkPlan2MiddleRow* middle_row;
    Q10FkPlan2RightRow* right_row;

    Q10FkPlan2Row(Q10FkPlan2LeftRow *left_row, Q10FkPlan2MiddleRow *middle_row, Q10FkPlan2RightRow *right_row) : left_row(left_row),
                                                                                   middle_row(middle_row),
                                                                                   right_row(right_row) {}
};

#endif
